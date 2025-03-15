import os
import logging
import base64
import json
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
    ConversationHandler,
)
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from concurrent.futures import ThreadPoolExecutor
import threading
import random
import time
import traceback

# Environment variables
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")

# Decode Google credentials
if GOOGLE_CREDENTIALS:
    creds_info = json.loads(base64.b64decode(GOOGLE_CREDENTIALS).decode("utf-8"))
    with open("credentials.json", "w") as creds_file:
        json.dump(creds_info, creds_file)

# Google Drive API setup
SCOPES = ["https://www.googleapis.com/auth/drive"]
MAX_THREADS = 10
BATCH_SIZE = 100

# Initialize logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Global variables
flow = None
service = None

# Conversation states
AUTH_CODE, SOURCE_FOLDER, DESTINATION_FOLDER, SEARCH_STRING, REPLACE_STRING = range(5)

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
                chat_id=self.update.effective_chat.id, text=f"Progress: {progress:.2f}%"
            )

def authenticate_google_drive():
    """Authenticate and return credentials."""
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
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
                pageToken=page_token,
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
        for file in files[i : i + BATCH_SIZE]:
            if search_str in file["name"]:
                new_name = file["name"].replace(search_str, replace_str)
                request = service.files().update(fileId=file["id"], body={"name": new_name})
                batch.add(request)
        if batch._requests:
            process_batch_with_retry(service, batch)
            progress_tracker.update_progress()

    # Process subfolders in parallel
    futures = []
    for folder in folders:
        futures.append(
            executor.submit(
                rename_files_in_folder,
                service,
                folder["id"],
                search_str,
                replace_str,
                executor,
                progress_tracker,
            )
        )
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
        "parents": [parent_folder_id] if parent_folder_id else [],
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

# Telegram Bot Commands
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start the bot and reset any ongoing conversation."""
    context.user_data.clear()
    await update.message.reply_text("Welcome to the Google Drive Manager Bot! Use /help to see all commands.")
    return ConversationHandler.END

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show help message."""
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
    """Start the Google Drive authorization flow."""
    global flow
    flow = InstalledAppFlow.from_client_secrets_file(
        "credentials.json", scopes=SCOPES, redirect_uri="urn:ietf:wg:oauth:2.0:oob"
    )
    auth_url, _ = flow.authorization_url(prompt="consent")
    await update.message.reply_text(
        f"🔑 **Authorization Required**\n\n"
        f"Please visit this link to authorize:\n{auth_url}\n\n"
        "After authorization, send the code you receive back here."
    )
    return AUTH_CODE

async def handle_auth_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the authorization code from the user."""
    global flow
    if not flow:
        await update.message.reply_text("⚠️ No active authorization session. Use /configuration first.")
        return

    code = update.message.text.strip()
    try:
        flow.fetch_token(code=code)
        creds = flow.credentials
        with open("token.json", "w") as token:
            token.write(creds.to_json())
        await update.message.reply_text("✅ Authorization successful! You can now use the bot.")
        # Clear user data to reset the state
        context.user_data.clear()
    except Exception as e:
        await update.message.reply_text(f"❌ Authorization failed: {str(e)}")
        logger.error(f"Authorization error: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
    return ConversationHandler.END

async def copy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start the /copy command and ask for the source folder URL."""
    await update.message.reply_text("Please send the source folder URL:")
    return SOURCE_FOLDER

async def source_folder_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the source folder URL and ask for the destination folder URL."""
    try:
        folder_id = extract_folder_id_from_url(update.message.text)
        context.user_data["source_folder_id"] = folder_id
        await update.message.reply_text("Please send the destination folder URL:")
        return DESTINATION_FOLDER
    except ValueError:
        await update.message.reply_text("❌ Invalid folder URL. Please try again.")
        return SOURCE_FOLDER

async def destination_folder_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the destination folder URL and start the copy process."""
    try:
        folder_id = extract_folder_id_from_url(update.message.text)
        context.user_data["destination_folder_id"] = folder_id
        source_folder_id = context.user_data["source_folder_id"]
        destination_folder_id = context.user_data["destination_folder_id"]

        total_items = count_files_in_folder(service, source_folder_id)
        progress_tracker = ProgressTracker(total_items, update, context)

        await update.message.reply_text(f"Copying {total_items} items...")
        new_folder_id = copy_folder(service, source_folder_id, destination_folder_id, progress_tracker)
        await update.message.reply_text(f"Folder copied successfully! New folder ID: {new_folder_id}")
    except ValueError:
        await update.message.reply_text("❌ Invalid folder URL. Please try again.")
        return DESTINATION_FOLDER
    return ConversationHandler.END

async def rename_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start the /rename command and ask for the folder URL."""
    await update.message.reply_text("Please send the folder URL:")
    return SOURCE_FOLDER

async def search_string_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the folder URL and ask for the search string."""
    try:
        folder_id = extract_folder_id_from_url(update.message.text)
        context.user_data["folder_id"] = folder_id
        await update.message.reply_text("Please send the search string:")
        return SEARCH_STRING
    except ValueError:
        await update.message.reply_text("❌ Invalid folder URL. Please try again.")
        return SOURCE_FOLDER

async def replace_string_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the search string and ask for the replace string."""
    context.user_data["search_str"] = update.message.text
    await update.message.reply_text("Please send the replace string:")
    return REPLACE_STRING

async def rename_files_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the replace string and start the rename process."""
    context.user_data["replace_str"] = update.message.text
    folder_id = context.user_data["folder_id"]
    search_str = context.user_data["search_str"]
    replace_str = context.user_data["replace_str"]

    total_items = count_files_in_folder(service, folder_id)
    progress_tracker = ProgressTracker(total_items, update, context)

    await update.message.reply_text(f"Renaming {total_items} items...")
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        rename_files_in_folder(service, folder_id, search_str, replace_str, executor, progress_tracker)
    await update.message.reply_text("Renaming completed!")
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel the current operation."""
    await update.message.reply_text("Operation cancelled.")
    context.user_data.clear()
    return ConversationHandler.END

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

    # Authorization handler
    auth_handler = ConversationHandler(
        entry_points=[CommandHandler("configuration", configuration)],
        states={
            AUTH_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_auth_code)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    application.add_handler(auth_handler)

    # Conversation handler for /copy command
    copy_handler = ConversationHandler(
        entry_points=[CommandHandler("copy", copy_command)],
        states={
            SOURCE_FOLDER: [MessageHandler(filters.TEXT & ~filters.COMMAND, source_folder_handler)],
            DESTINATION_FOLDER: [MessageHandler(filters.TEXT & ~filters.COMMAND, destination_folder_handler)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    # Conversation handler for /rename command
    rename_handler = ConversationHandler(
        entry_points=[CommandHandler("rename", rename_command)],
        states={
            SOURCE_FOLDER: [MessageHandler(filters.TEXT & ~filters.COMMAND, search_string_handler)],
            SEARCH_STRING: [MessageHandler(filters.TEXT & ~filters.COMMAND, replace_string_handler)],
            REPLACE_STRING: [MessageHandler(filters.TEXT & ~filters.COMMAND, rename_files_handler)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    application.add_handler(copy_handler)
    application.add_handler(rename_handler)

    # Start the bot
    application.run_polling()

if __name__ == "__main__":
    main()
