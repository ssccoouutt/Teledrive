import re
import os
import io
import random
import asyncio
import traceback
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.auth.transport.requests import Request
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# Configuration
BOT_TOKEN = "7846379611:AAGzu4KM-Aq699Q8aHNt29t0YbTnDKbkXbI"
TOKEN_PATH = 'token.json'
CREDENTIALS_PATH = 'credentials.json'
PHASE2_SOURCE = '1TaBiq6z01lLP-znWMz1S_RwJ1PkLRyjk'
PHASE3_SOURCE = '12V7EnRIYcSgEtt0PR5fhV8cO22nzYuiv'
SHORT_LINKS = ["rb.gy/cd8ugy", "bit.ly/3UcvhlA", "t.ly/CfcVB", "cutt.ly/Kee3oiLO"]
TARGET_CHANNEL = "@techworld196"
BANNED_FILE_ID = '1B5GAAtzpuH_XNGyUiJIMDlB9hJfxkg8r'
REPLACE_FILE_ID = '1HK79HS_a3lVZd30HEytp0flPqa1cIVn7'

# Initialize banned items from Google Drive
def initialize_banned_items(service):
    try:
        banned_file = service.files().get_media(fileId=BANNED_FILE_ID).execute()
        banned_items = banned_file.decode('utf-8').splitlines()
        return banned_items
    except Exception as e:
        print(f"Error initializing banned items: {str(e)}")
        return []

def save_banned_items(service, banned_items):
    try:
        banned_file_content = '\n'.join(banned_items).encode('utf-8')
        media_body = MediaIoBaseUpload(io.BytesIO(banned_file_content), mimetype='text/plain')
        service.files().update(fileId=BANNED_FILE_ID, media_body=media_body).execute()
    except Exception as e:
        print(f"Error saving banned items: {str(e)}")

def initialize_replace_rules(service):
    try:
        request = service.files().get_media(fileId=REPLACE_FILE_ID)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        
        replace_rules = []
        content = fh.getvalue().decode('utf-8')
        for line in content.splitlines():
            if line.strip() and '|' in line:
                old, new = line.split('|', 1)
                replace_rules.append((old.strip(), new.strip()))
        return replace_rules
    except Exception as e:
        print(f"Error initializing replace rules: {str(e)}")
        return []

def save_replace_rules(service, replace_rules):
    try:
        content = '\n'.join([f"{old}|{new}" for old, new in replace_rules])
        media_body = MediaIoBaseUpload(io.BytesIO(content.encode('utf-8')), mimetype='text/plain')
        service.files().update(fileId=REPLACE_FILE_ID, media_body=media_body).execute()
    except Exception as e:
        print(f"Error saving replace rules: {str(e)}")

def get_drive_service():
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            raise Exception('No valid credentials found.')
        with open(TOKEN_PATH, 'w') as token:
            token.write(creds.to_json())
    return build('drive', 'v3', credentials=creds)

def extract_folder_id(url):
    match = re.search(r'/folders/([a-zA-Z0-9-_]+)', url)
    return match.group(1) if match else None

def extract_file_id(url):
    match = re.search(r'/file/d/([a-zA-Z0-9-_]+)', url)
    return match.group(1) if match else None

def should_skip_item(name, banned_items):
    return name in banned_items

def copy_folder(service, folder_id, banned_items):
    try:
        folder = service.files().get(fileId=folder_id, fields='name').execute()
        new_folder = service.files().create(body={
            'name': folder['name'],
            'mimeType': 'application/vnd.google-apps.folder'
        }).execute()
        new_folder_id = new_folder['id']

        copy_folder_contents(service, folder_id, new_folder_id, banned_items)
        subfolders = get_all_subfolders_recursive(service, new_folder_id)
        for subfolder_id in subfolders:
            copy_files_only(service, PHASE2_SOURCE, subfolder_id, banned_items, overwrite=True)

        copy_bonus_content(service, PHASE3_SOURCE, new_folder_id, banned_items, overwrite=True)
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

def copy_files_only(service, source_id, dest_id, banned_items, overwrite=False):
    page_token = None
    while True:
        response = service.files().list(
            q=f"'{source_id}' in parents",
            fields='nextPageToken, files(id, name, mimeType)',
            pageToken=page_token
        ).execute()
        
        for item in response.get('files', []):
            if should_skip_item(item['name'], banned_items):
                continue
            if item['mimeType'] != 'application/vnd.google-apps.folder':
                copy_item_to_folder(service, item, dest_id, banned_items, overwrite)
        
        page_token = response.get('nextPageToken')
        if not page_token:
            break

def copy_bonus_content(service, source_id, dest_id, banned_items, overwrite=False):
    page_token = None
    while True:
        response = service.files().list(
            q=f"'{source_id}' in parents",
            fields='nextPageToken, files(id, name, mimeType)',
            pageToken=page_token
        ).execute()
        
        for item in response.get('files', []):
            if should_skip_item(item['name'], banned_items):
                continue
            copy_item_to_folder(service, item, dest_id, banned_items, overwrite)
        
        page_token = response.get('nextPageToken')
        if not page_token:
            break

def copy_item_to_folder(service, item, dest_folder_id, banned_items, overwrite=False):
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
            copy_bonus_content(service, item['id'], new_folder['id'], banned_items, overwrite)
        else:
            service.files().copy(
                fileId=item['id'],
                body={'parents': [dest_folder_id]}
            ).execute()
    except Exception as e:
        print(f"Error copying {item['name']}: {str(e)}")

def copy_folder_contents(service, source_id, dest_id, banned_items):
    page_token = None
    while True:
        response = service.files().list(
            q=f"'{source_id}' in parents",
            fields='nextPageToken, files(id, name, mimeType)',
            pageToken=page_token
        ).execute()
        
        for item in response.get('files', []):
            if should_skip_item(item['name'], banned_items):
                continue
                
            if item['mimeType'] == 'application/vnd.google-apps.folder':
                new_subfolder = service.files().create(body={
                    'name': item['name'],
                    'parents': [dest_id],
                    'mimeType': 'application/vnd.google-apps.folder'
                }).execute()
                copy_folder_contents(service, item['id'], new_subfolder['id'], banned_items)
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
                new_name = item['name'].replace('.mp4', ' (Telegram@TechZoneX).mp4')
                service.files().update(
                    fileId=item['id'],
                    body={'name': new_name}
                ).execute()
        
        page_token = response.get('nextPageToken')
        if not page_token:
            break

def process_content(original_text, drive_links, replace_rules):
    # Apply replacement rules
    for old_text, new_text in replace_rules:
        original_text = original_text.replace(old_text, new_text)
    
    # Replace drive links
    for old_url, new_url in drive_links:
        original_text = original_text.replace(old_url, new_url)
    
    # Clear everything after the last drive link + short URL
    if drive_links:
        # Find the position of the last replacement
        last_pos = 0
        for old_url, new_url in drive_links:
            pos = original_text.rfind(new_url)
            if pos != -1 and pos > last_pos:
                last_pos = pos + len(new_url)
        
        # Keep only up to the last replacement
        original_text = original_text[:last_pos]
    
    return original_text.strip()

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message or (message.text and message.text.startswith('/')):
        return

    original_text = message.caption or message.text or ''
    drive_links = []

    try:
        drive_service = get_drive_service()
        banned_items = initialize_banned_items(drive_service)
        replace_rules = initialize_replace_rules(drive_service)

        if original_text:
            url_matches = list(re.finditer(
                r'https?://drive\.google\.com/drive/folders/[\w-]+[^\s>]*',
                original_text
            ))
            
            for match in url_matches:
                url = match.group()
                folder_id = extract_folder_id(url)
                
                if folder_id:
                    try:
                        new_folder_id = await asyncio.get_event_loop().run_in_executor(
                                None, copy_folder, drive_service, folder_id, banned_items
                            )
                        random_link = random.choice(SHORT_LINKS)
                        new_url = f'https://drive.google.com/drive/folders/{new_folder_id} {random_link}'
                        drive_links.append((url, new_url))
                    except Exception as e:
                        await message.reply_text(f"⚠️ Error processing {url}: {str(e)}")
                        continue

            final_text = process_content(original_text, drive_links, replace_rules)

        send_args = {
            'chat_id': TARGET_CHANNEL,
            'disable_notification': True
        }

        if message.photo:
            await context.bot.send_photo(
                photo=message.photo[-1].file_id,
                caption=final_text,
                **send_args
            )
        elif message.video:
            await context.bot.send_video(
                video=message.video.file_id,
                caption=final_text,
                **send_args
            )
        elif message.document:
            await context.bot.send_document(
                document=message.document.file_id,
                caption=final_text,
                **send_args
            )
        elif message.audio:
            await context.bot.send_audio(
                audio=message.audio.file_id,
                caption=final_text,
                **send_args
            )
        else:
            await context.bot.send_message(
                text=final_text,
                **send_args
            )

    except Exception as e:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"⚠️ Processing error: {str(e)[:200]}"
        )

async def ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not context.args:
            await update.message.reply_text("❌ Usage: /ban <filename_or_folder_or_drive_link>")
            return

        input_text = ' '.join(context.args).strip()
        if not input_text:
            await update.message.reply_text("❌ Empty input provided")
            return

        drive_service = get_drive_service()
        banned_items = initialize_banned_items(drive_service)

        # Check if input is a Google Drive link
        file_id = extract_file_id(input_text)
        folder_id = extract_folder_id(input_text)
        
        item_name = input_text  # default to original input
        
        if file_id:
            try:
                file_info = drive_service.files().get(fileId=file_id, fields='name').execute()
                item_name = file_info['name']
            except Exception as e:
                await update.message.reply_text(f"⚠️ Could not fetch file info: {str(e)}")
                return
        elif folder_id:
            try:
                folder_info = drive_service.files().get(fileId=folder_id, fields='name').execute()
                item_name = folder_info['name']
            except Exception as e:
                await update.message.reply_text(f"⚠️ Could not fetch folder info: {str(e)}")
                return

        if item_name not in banned_items:
            banned_items.append(item_name)
            save_banned_items(drive_service, banned_items)
            response_text = f"✅ Banned: {item_name}"
        else:
            response_text = f"⚠️ Already banned: {item_name}"
        
        await update.message.reply_text(response_text)

    except Exception as e:
        await update.message.reply_text(f"⚠️ Ban failed: {str(e)}")

async def unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not context.args:
            await update.message.reply_text("❌ Usage: /unban <filename_or_folder_or_drive_link>")
            return

        input_text = ' '.join(context.args).strip()
        if not input_text:
            await update.message.reply_text("❌ Empty input provided")
            return

        drive_service = get_drive_service()
        banned_items = initialize_banned_items(drive_service)

        # Check if input is a Google Drive link
        file_id = extract_file_id(input_text)
        folder_id = extract_folder_id(input_text)
        
        item_name = input_text  # default to original input
        
        if file_id:
            try:
                file_info = drive_service.files().get(fileId=file_id, fields='name').execute()
                item_name = file_info['name']
            except Exception as e:
                await update.message.reply_text(f"⚠️ Could not fetch file info: {str(e)}")
                return
        elif folder_id:
            try:
                folder_info = drive_service.files().get(fileId=folder_id, fields='name').execute()
                item_name = folder_info['name']
            except Exception as e:
                await update.message.reply_text(f"⚠️ Could not fetch folder info: {str(e)}")
                return

        if item_name in banned_items:
            banned_items.remove(item_name)
            save_banned_items(drive_service, banned_items)
            response_text = f"✅ Unbanned: {item_name}"
        else:
            response_text = f"⚠️ Not banned: {item_name}"
        
        await update.message.reply_text(response_text)

    except Exception as e:
        await update.message.reply_text(f"⚠️ Unban failed: {str(e)}")

async def replace(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if len(context.args) < 2:
            await update.message.reply_text("❌ Usage: /replace <old_text> <new_text>")
            return

        old_text = ' '.join(context.args[:-1]).strip()
        new_text = context.args[-1].strip()

        if not old_text or not new_text:
            await update.message.reply_text("❌ Both old and new text must be provided")
            return

        drive_service = get_drive_service()
        replace_rules = initialize_replace_rules(drive_service)

        # Check if replacement rule already exists
        exists = any(old == old_text for old, new in replace_rules)
        
        if exists:
            # Update existing rule
            replace_rules = [(old, new_text if old == old_text else new) for old, new in replace_rules]
            response_text = f"✅ Updated replacement: '{old_text}' → '{new_text}'"
        else:
            # Add new rule
            replace_rules.append((old_text, new_text))
            response_text = f"✅ Added replacement: '{old_text}' → '{new_text}'"

        save_replace_rules(drive_service, replace_rules)
        await update.message.reply_text(response_text)

    except Exception as e:
        await update.message.reply_text(f"⚠️ Replace failed: {str(e)}")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    error = context.error
    tb_list = traceback.format_exception(type(error), error, error.__traceback__)
    tb_string = ''.join(tb_list)
    print(f"Exception occurred:\n{tb_string}")
    
    try:
        if update and update.effective_chat:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="⚠️ An error occurred. Please check the format and try again."
            )
    except Exception as e:
        print(f"Error in error handler while sending message: {e}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🚀 TechZoneX Auto Forward Bot\n\n"
        "Send any post with Google Drive links for processing!\n"
        "Admins:\n"
        "/ban <name_or_link> - Block files/folders\n"
        "/unban <name_or_link> - Unblock files/folders\n"
        "/replace <old> <new> - Replace text in posts"
    )

def main():
    application = Application.builder().token(BOT_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("ban", ban))
    application.add_handler(CommandHandler("unban", unban))
    application.add_handler(CommandHandler("replace", replace))
    
    application.add_handler(MessageHandler(
        filters.CAPTION | filters.TEXT | filters.PHOTO |
        filters.VIDEO | filters.Document.ALL | filters.AUDIO &
        ~filters.COMMAND,
        handle_message
    ))
    
    application.add_error_handler(error_handler)
    application.run_polling()

if __name__ == "__main__":
    main()
