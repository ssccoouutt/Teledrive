import re
import os
import json
import random
import asyncio
import base64
import logging
from collections import defaultdict
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import Flow
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# Configuration
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")  # Base64 encoded
SCOPES = ['https://www.googleapis.com/auth/drive']
CLIENT_SECRET_FILE = 'credentials.json'
TOKEN_FILE = 'token.json'
PHASE2_SOURCE = '1TaBiq6z01lLP-znWMz1S_RwJ1PkLRyjk'
PHASE3_SOURCE = '12V7EnRIYcSgEtt0PR5fhV8cO22nzYuiv'
SHORT_LINKS = ["rb.gy/cd8ugy", "bit.ly/3UcvhlA", "t.ly/CfcVB", "cutt.ly/Kee3oiLO"]
TARGET_CHANNEL = "@techworld196"
BANNED_ITEMS_FILE = 'banned_items.json'

# Initialize logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Global variables
flow = None
pending_authorizations = {}
processing_tasks = {}

# Initialize credentials file
if GOOGLE_CREDENTIALS and not os.path.exists(CLIENT_SECRET_FILE):
    try:
        with open(CLIENT_SECRET_FILE, 'w') as f:
            f.write(base64.b64decode(GOOGLE_CREDENTIALS).decode())
    except Exception as e:
        logger.error(f"Failed to create credentials.json: {e}")
        raise

def initialize_banned_items():
    default = {
        'files': ['100$ Free.docx', 'Free Courses.pdf'],
        'folders': ['00- Join LearnWithFaizan']
    }
    
    if not os.path.exists(BANNED_ITEMS_FILE):
        with open(BANNED_ITEMS_FILE, 'w') as f:
            json.dump(default, f)
        return default
    
    try:
        with open(BANNED_ITEMS_FILE, 'r') as f:
            data = json.load(f)
            if 'files' not in data or 'folders' not in data:
                raise ValueError("Invalid banned items structure")
            return data
    except (json.JSONDecodeError, ValueError):
        with open(BANNED_ITEMS_FILE, 'w') as f:
            json.dump(default, f)
        return default

banned_items = initialize_banned_items()

def save_banned_items():
    with open(BANNED_ITEMS_FILE, 'w') as f:
        json.dump(banned_items, f, indent=2)

def get_drive_service(user_id):
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                logger.error(f"Refresh error: {e}")
                return None
        else:
            return None
    
    return build('drive', 'v3', credentials=creds)

async def start_authorization(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global flow
    user_id = update.message.from_user.id
    
    try:
        flow = Flow.from_client_secrets_file(
            CLIENT_SECRET_FILE,
            scopes=SCOPES,
            redirect_uri='urn:ietf:wg:oauth:2.0:oob'
        )
        auth_url, _ = flow.authorization_url(prompt='consent')
        pending_authorizations[user_id] = flow
        
        await update.message.reply_text(
            f"🔑 Authorization required!\n\n"
            f"Please visit this link to authorize:\n{auth_url}\n\n"
            "After authorization, send the code you receive back here."
        )
    except Exception as e:
        logger.error(f"Authorization error: {e}")
        await update.message.reply_text("❌ Authorization setup failed. Please try again.")

async def handle_authorization_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    code = update.message.text.strip()
    
    if user_id not in pending_authorizations:
        await update.message.reply_text("⚠️ No pending authorization request. Start with /start")
        return
    
    try:
        flow = pending_authorizations[user_id]
        flow.fetch_token(code=code)
        
        with open(TOKEN_FILE, 'w') as token_file:
            token_file.write(flow.credentials.to_json())
        
        del pending_authorizations[user_id]
        await update.message.reply_text("✅ Authorization successful! You can now use Drive features.")
    except Exception as e:
        logger.error(f"Token exchange error: {e}")
        await update.message.reply_text("❌ Invalid authorization code. Please try again.")

def extract_folder_id(url):
    match = re.search(r'/folders/([a-zA-Z0-9-_]+)', url)
    return match.group(1) if match else None

def should_skip_item(name):
    return name in banned_items['files'] or name in banned_items['folders']

def copy_folder(service, folder_id):
    try:
        # Create main folder copy
        folder = service.files().get(fileId=folder_id, fields='name').execute()
        new_folder = service.files().create(body={
            'name': folder['name'],
            'mimeType': 'application/vnd.google-apps.folder'
        }).execute()
        new_folder_id = new_folder['id']

        # Phase 1: Copy original structure
        copy_folder_contents(service, folder_id, new_folder_id)

        # Phase 2: Add files to all subfolders
        subfolders = get_all_subfolders_recursive(service, new_folder_id)
        for subfolder_id in subfolders:
            copy_files_only(service, PHASE2_SOURCE, subfolder_id, overwrite=True)

        # Phase 3: Add content to root
        copy_bonus_content(service, PHASE3_SOURCE, new_folder_id, overwrite=True)

        # Rename video files
        rename_video_files(service, new_folder_id)
        for subfolder_id in subfolders:
            rename_video_files(service, subfolder_id)

        return new_folder_id
    except Exception as e:
        raise Exception(f"Copy failed: {str(e)}")

def get_all_subfolders_recursive(service, folder_id):
    subfolders = []
    queue = [folder_id]
    
    while queue:
        current_folder = queue.pop(0)
        page_token = None
        
        while True:
            response = service.files().list(
                q=f"'{current_folder}' in parents and mimeType='application/vnd.google-apps.folder'",
                fields='nextPageToken, files(id)',
                pageToken=page_token
            ).execute()
            
            new_folders = response.get('files', [])
            for folder in new_folders:
                subfolders.append(folder['id'])
                queue.append(folder['id'])
            
            page_token = response.get('nextPageToken')
            if not page_token:
                break
    
    return subfolders

def copy_files_only(service, source_id, dest_id, overwrite=False):
    page_token = None
    while True:
        response = service.files().list(
            q=f"'{source_id}' in parents",
            fields='nextPageToken, files(id, name, mimeType)',
            pageToken=page_token
        ).execute()
        
        for item in response.get('files', []):
            if should_skip_item(item['name']):
                continue
            if item['mimeType'] != 'application/vnd.google-apps.folder':
                copy_item_to_folder(service, item, dest_id, overwrite)
        
        page_token = response.get('nextPageToken')
        if not page_token:
            break

def copy_bonus_content(service, source_id, dest_id, overwrite=False):
    page_token = None
    while True:
        response = service.files().list(
            q=f"'{source_id}' in parents",
            fields='nextPageToken, files(id, name, mimeType)',
            pageToken=page_token
        ).execute()
        
        for item in response.get('files', []):
            if should_skip_item(item['name']):
                continue
            copy_item_to_folder(service, item, dest_id, overwrite)
        
        page_token = response.get('nextPageToken')
        if not page_token:
            break

def copy_item_to_folder(service, item, dest_folder_id, overwrite=False):
    try:
        if overwrite:
            existing = service.files().list(
                q=f"name='{item['name']}' and '{dest_folder_id}' in parents",
                fields='files(id)'
            ).execute().get('files', [])
            
            for file in existing:
                service.files().delete(fileId=file['id']).execute()

        if item['mimeType'] == 'application/vnd.google-apps.folder':
            new_folder = service.files().create(body={
                'name': item['name'],
                'parents': [dest_folder_id],
                'mimeType': 'application/vnd.google-apps.folder'
            }).execute()
            copy_bonus_content(service, item['id'], new_folder['id'], overwrite)
        else:
            service.files().copy(
                fileId=item['id'],
                body={'parents': [dest_folder_id]}
            ).execute()
    except Exception as e:
        print(f"Error copying {item['name']}: {str(e)}")

def copy_folder_contents(service, source_id, dest_id):
    page_token = None
    while True:
        response = service.files().list(
            q=f"'{source_id}' in parents",
            fields='nextPageToken, files(id, name, mimeType)',
            pageToken=page_token
        ).execute()
        
        for item in response.get('files', []):
            if should_skip_item(item['name']):
                continue
                
            if item['mimeType'] == 'application/vnd.google-apps.folder':
                new_subfolder = service.files().create(body={
                    'name': item['name'],
                    'parents': [dest_id],
                    'mimeType': 'application/vnd.google-apps.folder'
                }).execute()
                copy_folder_contents(service, item['id'], new_subfolder['id'])
            else:
                service.files().copy(
                    fileId=item['id'],
                    body={'parents': [dest_id]}
                ).execute()
        
        page_token = response.get('nextPageToken')
        if not page_token:
            break

def rename_video_files(service, folder_id):
    page_token = None
    while True:
        response = service.files().list(
            q=f"'{folder_id}' in parents and mimeType='video/mp4'",
            fields='nextPageToken, files(id,name)',
            pageToken=page_token
        ).execute()
        
        for item in response.get('files', []):
            if item['name'].endswith('.mp4'):
                new_name = item['name'].replace('.mp4', ' (Telegram@TechZoneX.mp4)')
                service.files().update(
                    fileId=item['id'],
                    body={'name': new_name}
                ).execute()
        
        page_token = response.get('nextPageToken')
        if not page_token:
            break

def convert_to_html(text, entities):
    html = text
    for entity in sorted(entities, key=lambda x: x.offset, reverse=True):
        start = entity.offset
        end = entity.offset + entity.length
        content = html[start:end]
        
        if entity.type == 'bold':
            replacement = f"<b>{content}</b>"
        elif entity.type == 'italic':
            replacement = f"<i>{content}</i>"
        elif entity.type == 'underline':
            replacement = f"<u>{content}</u>"
        elif entity.type == 'strikethrough':
            replacement = f"<s>{content}</s>"
        elif entity.type == 'code':
            replacement = f"<code>{content}</code>"
        elif entity.type == 'pre':
            replacement = f"<pre>{content}</pre>"
        elif entity.type == 'text_link':
            replacement = f'<a href="{entity.url}">{content}</a>'
        else:
            continue
        
        html = html[:start] + replacement + html[end:]
    return html

def process_content(original_html, drive_links):
    for old_url, new_url in drive_links:
        old_url_clean = re.escape(old_url)
        pattern = re.compile(
            fr'(<a\s[^>]*?href\s*=\s*["\']{old_url_clean}(?:[^"\'>]*?)["\'][^>]*>.*?</a>)|({old_url_clean}[^\s<]*)',
            flags=re.IGNORECASE
        )
        original_html = pattern.sub(new_url, original_html)
    
    # Clean residual HTML
    original_html = re.sub(r'<([a-z]+)([^>]*)(?<!/)>$', '', original_html)
    original_html = re.sub(r'<([a-z]+)([^>]*)></\1>', '', original_html)
    
    return original_html.strip()

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    user_id = update.message.from_user.id
    
    # Check for authorization code first
    if re.match(r'^\d+/[\w-]+$', message.text.strip()):
        await handle_authorization_code(update, context)
        return

    # Handle Drive links
    if 'drive.google.com' in message.text and '/folders/' in message.text:
        service = get_drive_service(user_id)
        if not service:
            await start_authorization(update, context)
            return
        
        try:
            original_text = message.caption or message.text or ''
            entities = message.caption_entities or message.entities or []
            drive_links = []
            
            url_matches = re.finditer(r'https?://drive\.google\.com/drive/folders/[\w-]+', original_text)
            for match in url_matches:
                url = match.group()
                folder_id = extract_folder_id(url)
                
                if folder_id:
                    try:
                        new_folder_id = await asyncio.get_event_loop().run_in_executor(
                            None, copy_folder, service, folder_id
                        )
                        random_link = random.choice(SHORT_LINKS)
                        new_url = f'https://drive.google.com/drive/folders/{new_folder_id} {random_link}'
                        drive_links.append((url, new_url))
                    except Exception as e:
                        await message.reply_text(f"⚠️ Error processing {url}: {str(e)}")
                        continue

            final_html = process_content(convert_to_html(original_text, entities), drive_links)

            send_args = {
                'chat_id': TARGET_CHANNEL,
                'parse_mode': 'HTML',
                'disable_notification': True
            }

            if message.photo:
                await context.bot.send_photo(
                    photo=message.photo[-1].file_id,
                    caption=final_html,
                    **send_args
                )
            elif message.video:
                await context.bot.send_video(
                    video=message.video.file_id,
                    caption=final_html,
                    **send_args
                )
            elif message.document:
                await context.bot.send_document(
                    document=message.document.file_id,
                    caption=final_html,
                    **send_args
                )
            elif message.audio:
                await context.bot.send_audio(
                    audio=message.audio.file_id,
                    caption=final_html,
                    **send_args
                )
            else:
                await context.bot.send_message(
                    text=final_html,
                    **send_args
                )

        except Exception as e:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"⚠️ Processing error: {str(e)[:200]}"
            )
    else:
        # Handle non-Drive link messages
        await context.bot.send_message(
            chat_id=TARGET_CHANNEL,
            text=message.text,
            parse_mode='HTML'
        )

async def ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not context.args:
            await update.message.reply_text("❌ Usage: /ban <filename_or_folder>")
            return

        item_name = ' '.join(context.args).strip()
        if not item_name:
            await update.message.reply_text("❌ Empty name provided")
            return

        if '.' in item_name.split('/')[-1]:
            if item_name not in banned_items['files']:
                banned_items['files'].append(item_name)
                response_text = f"✅ Banned file: {item_name}"
            else:
                response_text = f"⚠️ File already banned: {item_name}"
        else:
            if item_name not in banned_items['folders']:
                banned_items['folders'].append(item_name)
                response_text = f"✅ Banned folder: {item_name}"
            else:
                response_text = f"⚠️ Folder already banned: {item_name}"
        
        save_banned_items()
        await update.message.reply_text(response_text)

    except Exception as e:
        await update.message.reply_text(f"⚠️ Ban failed: {str(e)}")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    error = context.error
    logger.error(f"Error: {str(error)}")
    if update.effective_message:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="⚠️ An error occurred. Please check the format and try again."
        )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🚀 TechZoneX Auto Forward Bot\n\n"
        "Send any post with Google Drive links for processing!\n"
        "Admins: Use /ban <name> to block files/folders"
    )

def main():
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("ban", ban))
    
    # Message handler
    application.add_handler(MessageHandler(
        filters.CAPTION | filters.TEXT | filters.PHOTO |
        filters.VIDEO | filters.Document.ALL | filters.AUDIO &
        ~filters.COMMAND,
        handle_message
    ))
    
    application.add_error_handler(error_handler)
    application.run_polling()

if __name__ == "__main__":
    if not os.path.exists(BANNED_ITEMS_FILE):
        save_banned_items()
    main()
