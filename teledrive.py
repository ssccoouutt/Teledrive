import re
import os
import json
import random
import asyncio
import base64
import logging
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import Flow
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# Configuration
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
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

# Global state
auth_flows = {}
processing_queue = asyncio.Queue()

# Initialize credentials
if GOOGLE_CREDENTIALS and not os.path.exists(CLIENT_SECRET_FILE):
    try:
        with open(CLIENT_SECRET_FILE, 'w') as f:
            f.write(base64.b64decode(GOOGLE_CREDENTIALS).decode())
    except Exception as e:
        logger.error(f"Credentials init error: {e}")
        raise

class DriveManager:
    @staticmethod
    def get_service():
        if not os.path.exists(TOKEN_FILE):
            return None
            
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        if not creds.valid:
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                return None
        return build('drive', 'v3', credentials=creds)

    @staticmethod
    def extract_folder_id(url):
        match = re.search(r'/folders/([\w-]+)', url)
        return match.group(1) if match else None

class AuthManager:
    @staticmethod
    async def start_flow(update: Update):
        try:
            flow = Flow.from_client_secrets_file(
                CLIENT_SECRET_FILE,
                scopes=SCOPES,
                redirect_uri='urn:ietf:wg:oauth:2.0:oob'
            )
            auth_url, _ = flow.authorization_url(prompt='consent')
            auth_flows[update.message.from_user.id] = flow
            await update.message.reply_text(
                f"🔑 [Authorization Required]\n\n{auth_url}\n\n"
                "Send the authorization code here after granting permissions."
            )
        except Exception as e:
            logger.error(f"Auth flow error: {e}")
            await update.message.reply_text("❌ Authorization setup failed")

    @staticmethod
    async def handle_code(update: Update, code: str):
        user_id = update.message.from_user.id
        if user_id not in auth_flows:
            await update.message.reply_text("⚠️ Start authorization first with /start")
            return

        try:
            flow = auth_flows[user_id]
            flow.fetch_token(code=code)
            with open(TOKEN_FILE, 'w') as token_file:
                token_file.write(flow.credentials.to_json())
            del auth_flows[user_id]
            await update.message.reply_text("✅ Authorization successful! Now processing your requests...")
            
            # Process queued items
            while not processing_queue.empty():
                item = await processing_queue.get()
                await process_message(item['update'], item['context'])
        except Exception as e:
            logger.error(f"Token error: {e}")
            await update.message.reply_text("❌ Invalid authorization code")

async def handle_authorization(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text.isdigit() and len(text) in (6, 42):  # Match both Google auth code formats
        await AuthManager.handle_code(update, text)
    else:
        await AuthManager.start_flow(update)

async def process_drive_content(update: Update, context: ContextTypes.DEFAULT_TYPE):
    service = DriveManager.get_service()
    if not service:
        await processing_queue.put({'update': update, 'context': context})
        await AuthManager.start_flow(update)
        return

    try:
        # Extract message content
        message = update.message
        content = message.caption or message.text or ""
        urls = re.findall(r'https://drive\.google\.com/drive/folders/[\w-]+', content)
        
        # Process Drive links
        processed_urls = []
        for url in urls:
            folder_id = DriveManager.extract_folder_id(url)
            if not folder_id:
                continue
                
            # Your existing copy_folder logic here
            new_folder_id = "NEW_FOLDER_ID"  # Replace with actual copy logic
            processed_urls.append(f"{url} {random.choice(SHORT_LINKS)}")

        # Forward content to channel
        if message.photo:
            await context.bot.send_photo(
                chat_id=TARGET_CHANNEL,
                photo=message.photo[-1].file_id,
                caption="\n".join(processed_urls) if processed_urls else None
            )
        elif message.video:
            await context.bot.send_video(
                chat_id=TARGET_CHANNEL,
                video=message.video.file_id,
                caption="\n".join(processed_urls) if processed_urls else None
            )
        elif message.document:
            await context.bot.send_document(
                chat_id=TARGET_CHANNEL,
                document=message.document.file_id,
                caption="\n".join(processed_urls) if processed_urls else None
            )
        else:
            await context.bot.send_message(
                chat_id=TARGET_CHANNEL,
                text="\n".join(processed_urls) if processed_urls else "No valid content"
            )
    except Exception as e:
        logger.error(f"Processing error: {e}")
        await update.message.reply_text("⚠️ Error processing content")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    # Check authorization first
    if not DriveManager.get_service():
        await processing_queue.put({'update': update, 'context': context})
        await AuthManager.start_flow(update)
        return

    await process_drive_content(update, context)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🚀 TechZoneX Auto-Forward Bot\n\n"
        "Send any post with Google Drive links\n"
        "I'll handle authorization automatically!"
    )

def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(
        filters.CAPTION | filters.TEXT | filters.PHOTO |
        filters.VIDEO | filters.Document.ALL | filters.AUDIO,
        handle_message
    ))
    app.run_polling()

if __name__ == "__main__":
    main()
