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
    code = update.message.text.strip() if update.message.text else None
    
    if not code:
        await update.message.reply_text("⚠️ Please provide a valid authorization code.")
        return
    
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
    if not url:
        return None
    match = re.search(r'/folders/([a-zA-Z0-9-_]+)', url)
    return match.group(1) if match else None

def should_skip_item(name):
    return name in banned_items['files'] or name in banned_items['folders']

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message or (message.text and message.text.startswith('/')):
        return

    # Check for authorization code first
    if message.text and re.match(r'^\d+/[\w-]+$', message.text.strip()):
        await handle_authorization_code(update, context)
        return

    # Handle Drive links
    if message.text and 'drive.google.com' in message.text and '/folders/' in message.text:
        user_id = update.message.from_user.id
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
            text=message.text or "No valid content found.",
            parse_mode='HTML'
        )

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
