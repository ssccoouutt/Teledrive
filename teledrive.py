import os
import logging
import base64
import json
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from concurrent.futures import ThreadPoolExecutor
import threading
import random
import time

# Environment variables
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
GOOGLE_CREDENTIALS = os.getenv('GOOGLE_CREDENTIALS')

# Decode Google credentials
if GOOGLE_CREDENTIALS:
    creds_info = json.loads(base64.b64decode(GOOGLE_CREDENTIALS).decode('utf-8'))
    with open('credentials.json', 'w') as creds_file:
        json.dump(creds_info, creds_file)

# Google Drive API setup
SCOPES = ['https://www.googleapis.com/auth/drive']
MAX_THREADS = 10
BATCH_SIZE = 100

# Initialize logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Global variables
flow = None
service = None

class ProgressTracker:
    """Track progress of an operation."""
    def __init__(self, total_items, update, context):
        self.total_items = total_items
        self.completed_items = 0
        self.lock = threading.Lock()
        self.update = update
        self.context = context

    def update_progress(self):
        """Increment the completed items counter and send progress update."""
        with self.lock:
            self.completed_items += 1
            progress = (self.completed_items / self.total_items) * 100
            self.context.bot.send_message(
                chat_id=self.update.effective_chat.id,
                text=f"Progress: {progress:.2f}%"
            )

def authenticate_google_drive():
    """Authenticate and return credentials."""
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
    return creds

def extract_folder_id_from_url(url):
    """Extract folder ID from Google Drive URL."""
    if "folders/" in url:
        return url.split("folders/")[1].split("?")[0]
    elif "file/d/" in url:
        return url.split("file/d/")[1].split("/")[0]
    raise ValueError("Invalid Google Drive URL")

def list_folder_items(service, folder_id):
    """List all items in a folder with pagination handling."""
    items = []
    page_token = None
    while True:
        try:
            results = service.files().list(
                q=f"'{folder_id}' in parents",
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token
            ).execute()
            items.extend(results.get("files", []))
            page_token = results.get("nextPageToken")
            if not page_token:
                break
        except HttpError as e:
            logger.error(f"Listing error: {e}")
            break
    return items

def process_batch_with_retry(service, batch):
    """Execute batch request with retry logic."""
    for attempt in range(3):
        try:
            batch.execute()
            return
        except HttpError as e:
            if e.resp.status == 429:
                sleep_time = (2 ** attempt) + random.random()
                logger.warning(f"Rate limited. Retrying in {sleep_time:.2f}s")
                time.sleep(sleep_time)
            else:
                logger.error(f"Batch failed: {str(e)}")
                break

def rename_files_in_folder(service, folder_id, search_str, replace_str, executor, progress_tracker):
    """Fast rename using batch requests and multithreading."""
    items = list_folder_items(service, folder_id)
    files = [item for item in items if item["mimeType"] != "application/vnd.google-apps.folder"]
    folders = [item for item in items if item["mimeType"] == "application/vnd.google-apps.folder"]

    # Process files in batches
    for i in range(0, len(files), BATCH_SIZE):
        batch = service.new_batch_http_request()
        for file in files[i:i+BATCH_SIZE]:
            if search_str in file["name"]:
                new_name = file["name"].replace(search_str, replace_str)
                request = service.files().update(
                    fileId=file["id"],
                    body={"name": new_name}
                )
                batch.add(request)
        if batch._requests:
            process_batch_with_retry(service, batch)
            progress_tracker.update_progress()

    # Process subfolders in parallel
    futures = []
    for folder in folders:
        futures.append(executor.submit(
            rename_files_in_folder,
            service,
            folder["id"],
            search_str,
            replace_str,
            executor,
            progress_tracker
        ))
    for future in futures:
        future.result()

def count_files_in_folder(service, folder_id):
    """Recursively count files in a folder and its subfolders."""
    items = list_folder_items(service, folder_id)
    files = [item for item in items if item["mimeType"] != "application/vnd.google-apps.folder"]
    folders = [item for item in items if item["mimeType"] == "application/vnd.google-apps.folder"]

    # Count files in the current folder
    file_count = len(files)

    # Recursively count files in subfolders
    for folder in folders:
        file_count += count_files_in_folder(service, folder["id"])

    return file_count

def copy_folder(service, source_folder_id, parent_folder_id=None, progress_tracker=None):
    """Recursively copy a folder and its contents to a new location."""
    # Get the source folder's metadata
    source_folder = service.files().get(fileId=source_folder_id, fields="name").execute()
    folder_name = source_folder["name"]

    # Create the new folder in the destination
    new_folder_metadata = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_folder_id] if parent_folder_id else []
    }
    new_folder = service.files().create(body=new_folder_metadata, fields="id").execute()
    new_folder_id = new_folder["id"]

    # List all items in the source folder
    items = list_folder_items(service, source_folder_id)

    # Copy files and subfolders
    for item in items:
        if item["mimeType"] == "application/vnd.google-apps.folder":
            # Recursively copy subfolders
            copy_folder(service, item["id"], new_folder_id, progress_tracker)
        else:
            # Copy files
            file_metadata = {"name": item["name"], "parents": [new_folder_id]}
            service.files().copy(fileId=item["id"], body=file_metadata).execute()
            if progress_tracker:
                progress_tracker.update_progress()

    return new_folder_id

def copy_contents(service, source_folder_id, destination_folder_id, progress_tracker):
    """Copy contents of a folder (excluding the folder itself) to a destination folder."""
    items = list_folder_items(service, source_folder_id)

    for item in items:
        if item["mimeType"] == "application/vnd.google-apps.folder":
            # Recursively copy subfolders
            copy_folder(service, item["id"], destination_folder_id, progress_tracker)
        else:
            # Copy files
            file_metadata = {"name": item["name"], "parents": [destination_folder_id]}
            service.files().copy(fileId=item["id"], body=file_metadata).execute()
            if progress_tracker:
                progress_tracker.update_progress()

def copy_contents_to_subfolders(service, source_folder_id, destination_folder_id, progress_tracker):
    """Copy contents of a folder (excluding the folder itself) to another folder and its subfolders."""
    # List all items in the source folder
    source_items = list_folder_items(service, source_folder_id)

    # List all subfolders in the destination folder
    destination_items = list_folder_items(service, destination_folder_id)
    destination_subfolders = [item for item in destination_items if item["mimeType"] == "application/vnd.google-apps.folder"]

    # Copy files to the destination folder
    for item in source_items:
        if item["mimeType"] != "application/vnd.google-apps.folder":
            file_metadata = {"name": item["name"], "parents": [destination_folder_id]}
            service.files().copy(fileId=item["id"], body=file_metadata).execute()
            if progress_tracker:
                progress_tracker.update_progress()

    # Recursively copy files to subfolders
    for subfolder in destination_subfolders:
        copy_contents_to_subfolders(service, source_folder_id, subfolder["id"], progress_tracker)

# Telegram Bot Commands
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('Welcome to the Google Drive Manager Bot! Use /help to see all commands.')

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = """
    Available commands:
    /start - Start the bot
    /help - Show this help message
    /configuration - Authorize Google Drive
    /copy - Copy Google Drive Folder
    /rename - Fast Rename Files (Search & Replace)
    /count - Count Files
    /copy_contents - Copy contents to another folder
    /copy_to_subfolders - Copy contents to another folder and its subfolders
    """
    await update.message.reply_text(help_text)

async def configuration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global flow
    flow = InstalledAppFlow.from_client_secrets_file(
        'credentials.json',
        scopes=SCOPES,
        redirect_uri='urn:ietf:wg:oauth:2.0:oob'
    )
    auth_url, _ = flow.authorization_url(prompt='consent')
    await update.message.reply_text(
        f"🔑 **Authorization Required**\n\n"
        f"Please visit this link to authorize:\n{auth_url}\n\n"
        "After authorization, send the code you receive back here."
    )

async def handle_auth_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global flow
    if not flow:
        await update.message.reply_text("⚠️ No active authorization session. Use /configuration first.")
        return

    code = update.message.text.strip()
    try:
        flow.fetch_token(code=code)
        creds = flow.credentials
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
        await update.message.reply_text("✅ Authorization successful! You can now use the bot.")
    except Exception as e:
        await update.message.reply_text(f"❌ Authorization failed: {str(e)}")
        logger.error(f"Authorization error: {e}")

async def copy_folder_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    source_url = context.args[0]
    destination_url = context.args[1] if len(context.args) > 1 else None
    source_id = extract_folder_id_from_url(source_url)
    destination_id = extract_folder_id_from_url(destination_url) if destination_url else None

    total_items = count_files_in_folder(service, source_id)
    progress_tracker = ProgressTracker(total_items, update, context)

    await update.message.reply_text(f"Copying {total_items} items...")
    new_folder_id = copy_folder(service, source_id, destination_id, progress_tracker)
    await update.message.reply_text(f"Folder copied successfully! New folder ID: {new_folder_id}")

async def rename_files_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    folder_url = context.args[0]
    search_str = context.args[1]
    replace_str = context.args[2]
    folder_id = extract_folder_id_from_url(folder_url)

    total_items = count_files_in_folder(service, folder_id)
    progress_tracker = ProgressTracker(total_items, update, context)

    await update.message.reply_text(f"Renaming {total_items} items...")
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        rename_files_in_folder(service, folder_id, search_str, replace_str, executor, progress_tracker)
    await update.message.reply_text("Renaming completed!")

async def count_files_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    folder_url = context.args[0]
    folder_id = extract_folder_id_from_url(folder_url)
    total_files = count_files_in_folder(service, folder_id)
    await update.message.reply_text(f"Total files in folder and subfolders: {total_files}")

async def copy_contents_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    source_url = context.args[0]
    destination_url = context.args[1]
    source_id = extract_folder_id_from_url(source_url)
    destination_id = extract_folder_id_from_url(destination_url)

    total_items = count_files_in_folder(service, source_id)
    progress_tracker = ProgressTracker(total_items, update, context)

    await update.message.reply_text(f"Copying {total_items} items...")
    copy_contents(service, source_id, destination_id, progress_tracker)
    await update.message.reply_text("Contents copied successfully!")

async def copy_to_subfolders_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    source_url = context.args[0]
    destination_url = context.args[1]
    source_id = extract_folder_id_from_url(source_url)
    destination_id = extract_folder_id_from_url(destination_url)

    total_items = count_files_in_folder(service, source_id)
    progress_tracker = ProgressTracker(total_items, update, context)

    await update.message.reply_text(f"Copying {total_items} items...")
    copy_contents_to_subfolders(service, source_id, destination_id, progress_tracker)
    await update.message.reply_text("Contents copied to subfolders successfully!")

def main():
    global service
    creds = authenticate_google_drive()
    if creds:
        service = build("drive", "v3", credentials=creds)

    # Build the Telegram bot
    application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("configuration", configuration))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_auth_code))
    application.add_handler(CommandHandler("copy", copy_folder_command))
    application.add_handler(CommandHandler("rename", rename_files_command))
    application.add_handler(CommandHandler("count", count_files_command))
    application.add_handler(CommandHandler("copy_contents", copy_contents_command))
    application.add_handler(CommandHandler("copy_to_subfolders", copy_to_subfolders_command))

    # Start the bot
    application.run_polling()

if __name__ == '__main__':
    main()
