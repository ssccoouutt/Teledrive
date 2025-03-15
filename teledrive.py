import os
import base64
import json
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, ConversationHandler

# Configuration
SCOPES = ["https://www.googleapis.com/auth/drive"]
MAX_THREADS = 10
BATCH_SIZE = 100
UNPOSTED_FOLDER_ID = "14tf687_8F4o2oYJTqyCZmvJjq45jRliy"
SECOND_SOURCE_FOLDER_ID = "12V7EnRIYcSgEtt0PR5fhV8cO22nzYuiv"

# Telegram Bot Token from environment variable
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# Decode credentials.json from Base64
CREDENTIALS_JSON = base64.b64decode(os.getenv("CREDENTIALS_JSON")).decode("utf-8")
with open("credentials.json", "w") as cred_file:
    cred_file.write(CREDENTIALS_JSON)

# Global variables for conversation states
AUTH_CODE, FOLDER_URL, SEARCH_STR, REPLACE_STR, SOURCE_URL, DESTINATION_URL = range(6)


class ProgressTracker:
    """Track progress of an operation."""

    def __init__(self, total_items):
        self.total_items = total_items
        self.completed_items = 0
        self.lock = threading.Lock()

    def update(self):
        """Increment the completed items counter."""
        with self.lock:
            self.completed_items += 1
            self.display_progress()

    def display_progress(self):
        """Display the current progress percentage."""
        progress = (self.completed_items / self.total_items) * 100
        print(f"Progress: {progress:.2f}%", end="\r")


def authenticate_google_drive():
    """Authenticate and return credentials."""
    creds = None
    if os.path.exists("token.json"):
        try:
            creds = Credentials.from_authorized_user_file("token.json", SCOPES)
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
        except Exception as e:
            print(f"Error loading credentials: {e}")
            os.remove("token.json")  # Delete invalid token file
            creds = None

    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
        auth_url, _ = flow.authorization_url(prompt="consent")
        return flow, auth_url
    return creds, None


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
            print(f"Listing error: {e}")
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
                sleep_time = (2**attempt) + random.random()
                print(f"Rate limited. Retrying in {sleep_time:.2f}s")
                time.sleep(sleep_time)
            else:
                print(f"Batch failed: {str(e)}")
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
            progress_tracker.update()

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
                progress_tracker.update()

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
                progress_tracker.update()


def copy_contents_to_subfolders(service, source_folder_id, destination_folder_id, progress_tracker):
    """Copy contents of a folder (excluding the folder itself) to another folder and its subfolders."""
    # List all items in the source folder
    source_items = list_folder_items(service, source_folder_id)

    # List all subfolders in the destination folder
    destination_items = list_folder_items(service, destination_folder_id)
    destination_subfolders = [
        item for item in destination_items if item["mimeType"] == "application/vnd.google-apps.folder"
    ]

    # Copy files to the destination folder
    for item in source_items:
        if item["mimeType"] != "application/vnd.google-apps.folder":
            file_metadata = {"name": item["name"], "parents": [destination_folder_id]}
            service.files().copy(fileId=item["id"], body=file_metadata).execute()
            if progress_tracker:
                progress_tracker.update()

    # Recursively copy files to subfolders
    for subfolder in destination_subfolders:
        copy_contents_to_subfolders(service, source_folder_id, subfolder["id"], progress_tracker)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command handler."""
    await update.message.reply_text(
        "Welcome! Use /configuration to authenticate Google Drive.\n"
        "Available commands:\n"
        "/copy_folder - Copy a Google Drive folder\n"
        "/rename_files - Rename files in a folder\n"
        "/count_files - Count files in a folder\n"
        "/copy_and_rename - Copy and rename files from two source folders\n"
        "/copy_contents - Copy contents to another folder\n"
    )


async def configuration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start the authentication process."""
    flow, auth_url = authenticate_google_drive()
    if auth_url:
        context.user_data["flow"] = flow
        await update.message.reply_text(f"Please visit this URL to authorize the application: {auth_url}")
        await update.message.reply_text("After authorization, paste the code you received here.")
        return AUTH_CODE
    else:
        await update.message.reply_text("Already authenticated.")
        return ConversationHandler.END


async def auth_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the authorization code."""
    user_code = update.message.text
    flow = context.user_data.get("flow")
    if flow:
        try:
            flow.fetch_token(code=user_code)
            creds = flow.credentials
            with open("token.json", "w") as token:
                token.write(creds.to_json())
            await update.message.reply_text("Authorization successful!")
        except Exception as e:
            await update.message.reply_text(f"Failed to authenticate: {e}")
    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel the current operation."""
    await update.message.reply_text("Operation cancelled.")
    return ConversationHandler.END


async def copy_folder_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the copy folder command."""
    await update.message.reply_text("Enter the source folder URL:")
    return FOLDER_URL


async def handle_folder_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the folder URL input."""
    folder_url = update.message.text
    context.user_data["folder_url"] = folder_url
    await update.message.reply_text("Enter the destination folder URL (leave blank for root):")
    return DESTINATION_URL


async def handle_destination_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the destination folder URL input."""
    destination_url = update.message.text
    folder_url = context.user_data.get("folder_url")
    source_folder_id = extract_folder_id_from_url(folder_url)
    destination_folder_id = extract_folder_id_from_url(destination_url) if destination_url else None

    # Authenticate Google Drive
    creds, _ = authenticate_google_drive()
    service = build("drive", "v3", credentials=creds)

    # Count total items to copy
    total_items = count_files_in_folder(service, source_folder_id)
    progress_tracker = ProgressTracker(total_items)

    await update.message.reply_text(f"Copying {total_items} items...")
    new_folder_id = copy_folder(service, source_folder_id, destination_folder_id, progress_tracker)
    await update.message.reply_text(f"\nFolder copied successfully! New folder ID: {new_folder_id}")
    return ConversationHandler.END


async def rename_files_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the rename files command."""
    await update.message.reply_text("Enter the folder URL:")
    return FOLDER_URL


async def handle_rename_folder_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the folder URL input for renaming."""
    folder_url = update.message.text
    context.user_data["folder_url"] = folder_url
    await update.message.reply_text("Enter the search string:")
    return SEARCH_STR


async def handle_search_str(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the search string input."""
    search_str = update.message.text
    context.user_data["search_str"] = search_str
    await update.message.reply_text("Enter the replace string:")
    return REPLACE_STR


async def handle_replace_str(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the replace string input."""
    replace_str = update.message.text
    folder_url = context.user_data.get("folder_url")
    search_str = context.user_data.get("search_str")

    # Authenticate Google Drive
    creds, _ = authenticate_google_drive()
    service = build("drive", "v3", credentials=creds)

    folder_id = extract_folder_id_from_url(folder_url)

    # Count total items to rename
    total_items = count_files_in_folder(service, folder_id)
    progress_tracker = ProgressTracker(total_items)

    await update.message.reply_text(f"Renaming {total_items} items...")
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        rename_files_in_folder(service, folder_id, search_str, replace_str, executor, progress_tracker)
    await update.message.reply_text("\nRenaming completed at high speed!")
    return ConversationHandler.END


async def count_files_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the count files command."""
    await update.message.reply_text("Enter the folder URL:")
    return FOLDER_URL


async def handle_count_folder_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the folder URL input for counting files."""
    folder_url = update.message.text
    folder_id = extract_folder_id_from_url(folder_url)

    # Authenticate Google Drive
    creds, _ = authenticate_google_drive()
    service = build("drive", "v3", credentials=creds)

    total_files = count_files_in_folder(service, folder_id)
    await update.message.reply_text(f"Total files in folder and subfolders: {total_files}")
    return ConversationHandler.END


async def copy_and_rename_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the copy and rename command."""
    await update.message.reply_text("Enter the first source folder URL:")
    return SOURCE_URL


async def handle_first_source_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the first source folder URL input."""
    source_url = update.message.text
    context.user_data["first_source_url"] = source_url

    # Authenticate Google Drive
    creds, _ = authenticate_google_drive()
    service = build("drive", "v3", credentials=creds)

    first_source_id = extract_folder_id_from_url(source_url)
    second_source_id = SECOND_SOURCE_FOLDER_ID

    # Count total items to copy
    total_items = count_files_in_folder(service, first_source_id) + count_files_in_folder(service, second_source_id)
    progress_tracker = ProgressTracker(total_items)

    await update.message.reply_text("Copying first source folder to 'Unposted'...")
    new_folder_id = copy_folder(service, first_source_id, UNPOSTED_FOLDER_ID, progress_tracker)

    await update.message.reply_text("Copying contents of second source folder to the newly copied folder...")
    copy_contents(service, second_source_id, new_folder_id, progress_tracker)

    await update.message.reply_text("Renaming .mp4 files...")
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        rename_files_in_folder(service, new_folder_id, ".mp4", " Telegram@TechZoneX.mp4", executor, progress_tracker)

    await update.message.reply_text("\nCopy and rename completed successfully!")
    return ConversationHandler.END


async def copy_contents_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the copy contents command."""
    await update.message.reply_text("Enter the source folder URL:")
    return SOURCE_URL


async def handle_copy_contents_source_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the source folder URL input for copying contents."""
    source_url = update.message.text
    context.user_data["source_url"] = source_url
    await update.message.reply_text("Enter the destination folder URL:")
    return DESTINATION_URL


async def handle_copy_contents_destination_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the destination folder URL input for copying contents."""
    destination_url = update.message.text
    source_url = context.user_data.get("source_url")
    source_folder_id = extract_folder_id_from_url(source_url)
    destination_folder_id = extract_folder_id_from_url(destination_url)

    # Authenticate Google Drive
    creds, _ = authenticate_google_drive()
    service = build("drive", "v3", credentials=creds)

    # Count total items to copy
    total_items = count_files_in_folder(service, source_folder_id)
    progress_tracker = ProgressTracker(total_items)

    await update.message.reply_text(f"Copying {total_items} items...")
    copy_contents_to_subfolders(service, source_folder_id, destination_folder_id, progress_tracker)
    await update.message.reply_text("\nContents copied successfully!")
    return ConversationHandler.END


def main():
    """Start the bot."""
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Conversation handler for authentication
    auth_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("configuration", configuration)],
        states={
            AUTH_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, auth_code)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    # Conversation handler for copying folders
    copy_folder_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("copy_folder", copy_folder_command)],
        states={
            FOLDER_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_folder_url)],
            DESTINATION_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_destination_url)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    # Conversation handler for renaming files
    rename_files_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("rename_files", rename_files_command)],
        states={
            FOLDER_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_rename_folder_url)],
            SEARCH_STR: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_search_str)],
            REPLACE_STR: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_replace_str)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    # Conversation handler for counting files
    count_files_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("count_files", count_files_command)],
        states={
            FOLDER_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_count_folder_url)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    # Conversation handler for copy and rename
    copy_and_rename_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("copy_and_rename", copy_and_rename_command)],
        states={
            SOURCE_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_first_source_url)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    # Conversation handler for copying contents
    copy_contents_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("copy_contents", copy_contents_command)],
        states={
            SOURCE_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_copy_contents_source_url)],
            DESTINATION_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_copy_contents_destination_url)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    # Add all handlers to the application
    application.add_handler(CommandHandler("start", start))
    application.add_handler(auth_conv_handler)
    application.add_handler(copy_folder_conv_handler)
    application.add_handler(rename_files_conv_handler)
    application.add_handler(count_files_conv_handler)
    application.add_handler(copy_and_rename_conv_handler)
    application.add_handler(copy_contents_conv_handler)

    # Start the bot
    application.run_polling()


if __name__ == "__main__":
    main()