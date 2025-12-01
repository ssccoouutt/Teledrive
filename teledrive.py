import re
import os
import io
import random
import asyncio
import traceback
import time
import aiohttp
import logging
import signal
from datetime import datetime
from aiohttp import web
from telegram import Update, MessageEntity
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, ConversationHandler
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow

# Configuration
BOT_TOKEN = "7846379611:AAFk9kkoQwsA6fCS4vF4Ltr6xn1W645nHFM"
TOKEN_PATH = 'token.json'
CREDENTIALS_PATH = 'credentials.json'
PHASE2_SOURCE = '1ixJU6s6bKbzIdsbjKDKrZYLt1nl_TSul'
PHASE3_SOURCE = '1iM6ghIcYsx1gIvfdjm-HjCRW3MWy0JCP'
# Short links removed as requested
TARGET_CHANNEL = "@techworld196"
BANNED_FILE_ID = '1r2BpwG9isOkKjL5tYj3WqqiF5w4oWpCY'
SCOPES = ['https://www.googleapis.com/auth/drive']

# Web Server Configuration
WEB_PORT = 8000
PING_INTERVAL = 25
HEALTH_CHECK_ENDPOINT = "/health"

# Constants
MAX_RETRIES = 5
CHUNK_SIZE = 20

# Authorization state
AUTH_STATE = 1
pending_authorizations = {}

# Global variables for web server
runner = None
site = None

# Initialize logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('aiohttp.access').setLevel(logging.WARNING)
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

def get_drive_service():
    """Initialize and return Google Drive service"""
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(TOKEN_PATH, 'w') as token:
                token.write(creds.to_json())
        else:
            raise Exception('Google Drive authorization required. Use /auth to authenticate.')
    
    return build('drive', 'v3', credentials=creds)

async def health_check(request):
    return web.Response(text=f"Bot is operational", status=200)

async def root_handler(request):
    return web.Response(text="Bot is running", status=200)

async def self_ping():
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f'http://localhost:{WEB_PORT}{HEALTH_CHECK_ENDPOINT}') as resp:
                    pass 
        except Exception:
            pass
        await asyncio.sleep(PING_INTERVAL)

async def run_webserver():
    app = web.Application()
    app.router.add_get(HEALTH_CHECK_ENDPOINT, health_check)
    app.router.add_get("/", root_handler)
    global runner, site
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', WEB_PORT)
    await site.start()
    logger.info(f"Health check server running on port {WEB_PORT}")

async def auth_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    flow = InstalledAppFlow.from_client_secrets_file(
        CREDENTIALS_PATH, scopes=SCOPES, redirect_uri='http://localhost:8080'
    )
    auth_url, _ = flow.authorization_url(prompt='consent')
    pending_authorizations[update.effective_user.id] = flow
    await update.message.reply_text(
        f"[Authorize Google Drive]({auth_url})\n\nSend me the redirected URL.",
        parse_mode='Markdown',
        disable_web_page_preview=True
    )
    return AUTH_STATE

async def handle_auth_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()
    code = None
    if 'code=' in text:
        code = text.split('code=')[1].split('&')[0]
    elif 'localhost' in text and '?code=' in text:
        code = text.split('?code=')[1].split('&')[0]
    
    if not code or user_id not in pending_authorizations:
        await update.message.reply_text("❌ Invalid URL")
        return ConversationHandler.END
    
    try:
        flow = pending_authorizations[user_id]
        flow.fetch_token(code=code)
        creds = flow.credentials
        with open(TOKEN_PATH, 'w') as token_file:
            token_file.write(creds.to_json())
        del pending_authorizations[user_id]
        await update.message.reply_text("✅ Authorization successful!")
    except Exception as e:
        await update.message.reply_text(f"❌ Failed: {str(e)}")
    return ConversationHandler.END

async def cancel_auth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in pending_authorizations:
        del pending_authorizations[user_id]
    await update.message.reply_text("❌ Cancelled")
    return ConversationHandler.END

def initialize_banned_items(service):
    try:
        request = service.files().get_media(fileId=BANNED_FILE_ID)
        banned_file = request.execute().decode('utf-8')
        sections = banned_file.split('\n\n')
        banned_data = {'names': [], 'size_types': [], 'rename_rules': []}
        for section in sections:
            if section.startswith('#BANNED_NAMES'):
                banned_data['names'] = section.split('\n')[1:]
            elif section.startswith('#BANNED_SIZE_TYPE'):
                banned_data['size_types'] = section.split('\n')[1:]
            elif section.startswith('#RENAME_RULES'):
                banned_data['rename_rules'] = section.split('\n')[1:]
        return banned_data
    except Exception as e:
        logger.error(f"Error loading banned items: {str(e)}")
        return {'names': [], 'size_types': [], 'rename_rules': []}

def save_banned_items(service, banned_data):
    try:
        content = ""
        if banned_data['names']: content += "#BANNED_NAMES\n" + "\n".join(banned_data['names']) + "\n\n"
        if banned_data['size_types']: content += "#BANNED_SIZE_TYPE\n" + "\n".join(banned_data['size_types']) + "\n\n"
        if banned_data['rename_rules']: content += "#RENAME_RULES\n" + "\n".join(banned_data['rename_rules'])
        media = MediaIoBaseUpload(io.BytesIO(content.strip().encode('utf-8')), mimetype='text/plain')
        service.files().update(fileId=BANNED_FILE_ID, media_body=media).execute()
        logger.info("Banned items saved.")
    except Exception as e:
        logger.error(f"Error saving banned items: {str(e)}")

def should_skip_item(name, mime_type, size, banned_data):
    if name in banned_data['names']:
        logger.info(f"Skipping banned name: {name}")
        return True
    size_type_str = f"{size}:{mime_type}"
    if size_type_str in banned_data['size_types']:
        logger.info(f"Skipping banned size/type: {size_type_str}")
        return True
    return False

def apply_rename_rules(name, rename_rules):
    for rule in rename_rules:
        if '|' in rule:
            old, new = rule.split('|', 1)
            name = name.replace(old, new)
    return name

def execute_with_retry(func, *args, **kwargs):
    func_name = getattr(func, '__name__', str(func))
    for attempt in range(MAX_RETRIES):
        try:
            return func(*args, **kwargs).execute()
        except HttpError as e:
            wait_time = 5 * (2 ** attempt) 
            if e.resp.status in [403, 429]:
                logger.warning(f"⚠️ RATE LIMIT in {func_name}. Waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            if e.resp.status in [500, 502, 503, 504]:
                logger.warning(f"⚠️ Server Error {e.resp.status} in {func_name}. Waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            logger.error(f"API Error in {func_name}: {e}")
            raise
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = 5 * (2 ** attempt)
                logger.warning(f"⚠️ Network error in {func_name}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
            logger.error(f"Critical error in {func_name}: {e}")
            raise
    raise Exception(f"Operation {func_name} failed after {MAX_RETRIES} attempts.")

def copy_file(service, file_id, banned_data):
    try:
        logger.info(f"STEP: Processing single file {file_id}")
        file = execute_with_retry(service.files().get, fileId=file_id, fields='name,mimeType,size')
        new_name = apply_rename_rules(file['name'], banned_data['rename_rules'])
        if should_skip_item(new_name, file['mimeType'], file.get('size', 0), banned_data):
            raise Exception(f"File {new_name} is banned")
        copied_file = service.files().copy(fileId=file_id).execute()
        logger.info(f"File copied: {new_name}")
        return copied_file['id']
    except Exception as e:
        logger.error(f"FAILED to copy file {file_id}: {str(e)}")
        raise

def copy_folder(service, folder_id, banned_data):
    try:
        logger.info(f"STEP: Starting folder copy: {folder_id}")
        folder = execute_with_retry(service.files().get, fileId=folder_id, fields='name')
        new_folder_name = apply_rename_rules(folder['name'], banned_data['rename_rules'])
        logger.info(f"Creating root folder: {new_folder_name}")
        new_folder = service.files().create(body={'name': new_folder_name, 'mimeType': 'application/vnd.google-apps.folder'}).execute()
        new_folder_id = new_folder['id']

        copy_folder_contents(service, folder_id, new_folder_id, banned_data)
        subfolders = get_all_subfolders_recursive(service, new_folder_id)
        
        logger.info(f"Phase 2: Adding watermark files to {len(subfolders)} subfolders...")
        for i, subfolder_id in enumerate(subfolders):
            copy_files_only(service, PHASE2_SOURCE, subfolder_id, banned_data, overwrite=True)
            if i % 10 == 0: time.sleep(1)

        logger.info("Phase 3: Adding bonus content...")
        copy_bonus_content(service, PHASE3_SOURCE, new_folder_id, banned_data, overwrite=True)
        
        logger.info("Phase 4: Renaming items...")
        rename_files_and_folders(service, new_folder_id, banned_data['rename_rules'])
        for i, subfolder_id in enumerate(subfolders):
            rename_files_and_folders(service, subfolder_id, banned_data['rename_rules'])
            if i % 10 == 0: time.sleep(1)

        logger.info("COMPLETED successfully.")
        return new_folder_id
    except Exception as e:
        logger.error(f"CRITICAL FAILURE in copy_folder: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def get_all_subfolders_recursive(service, folder_id):
    subfolders = []
    queue = [folder_id]
    while queue:
        current_folder = queue.pop(0)
        page_token = None
        while True:
            try:
                response = execute_with_retry(service.files().list, q=f"'{current_folder}' in parents and mimeType='application/vnd.google-apps.folder'", fields='nextPageToken, files(id)', pageSize=CHUNK_SIZE, pageToken=page_token)
                for folder in response.get('files', []):
                    subfolders.append(folder['id'])
                    queue.append(folder['id'])
                page_token = response.get('nextPageToken')
                if not page_token: break
            except Exception as e:
                logger.error(f"Error getting subfolders: {str(e)}")
                break
    return subfolders

def copy_files_only(service, source_id, dest_id, banned_data, overwrite=False):
    page_token = None
    while True:
        try:
            response = execute_with_retry(service.files().list, q=f"'{source_id}' in parents", fields='nextPageToken, files(id, name, mimeType, size)', pageSize=CHUNK_SIZE, pageToken=page_token)
            for item in response.get('files', []):
                new_name = apply_rename_rules(item['name'], banned_data['rename_rules'])
                if should_skip_item(new_name, item['mimeType'], item.get('size', 0), banned_data): continue
                if item['mimeType'] != 'application/vnd.google-apps.folder':
                    copy_item_to_folder(service, item, dest_id, banned_data, overwrite)
            page_token = response.get('nextPageToken')
            if not page_token: break
        except Exception: break

def copy_bonus_content(service, source_id, dest_id, banned_data, overwrite=False):
    page_token = None
    while True:
        try:
            response = execute_with_retry(service.files().list, q=f"'{source_id}' in parents", fields='nextPageToken, files(id, name, mimeType, size)', pageSize=CHUNK_SIZE, pageToken=page_token)
            for item in response.get('files', []):
                new_name = apply_rename_rules(item['name'], banned_data['rename_rules'])
                if should_skip_item(new_name, item['mimeType'], item.get('size', 0), banned_data): continue
                copy_item_to_folder(service, item, dest_id, banned_data, overwrite)
            page_token = response.get('nextPageToken')
            if not page_token: break
        except Exception: break

def copy_item_to_folder(service, item, dest_folder_id, banned_data, overwrite=False):
    try:
        new_name = apply_rename_rules(item['name'], banned_data['rename_rules'])
        if overwrite:
            existing = execute_with_retry(service.files().list, q=f"name='{new_name}' and '{dest_folder_id}' in parents", fields='files(id)').get('files', [])
            for file in existing: execute_with_retry(service.files().delete, fileId=file['id'])
        if item['mimeType'] == 'application/vnd.google-apps.folder':
            new_folder = service.files().create(body={'name': new_name, 'parents': [dest_folder_id], 'mimeType': 'application/vnd.google-apps.folder'}).execute()
            copy_bonus_content(service, item['id'], new_folder['id'], banned_data, overwrite)
        else:
            service.files().copy(fileId=item['id'], body={'parents': [dest_folder_id]}).execute()
    except Exception as e: logger.error(f"Error copying {item['name']}: {str(e)}")

def copy_folder_contents(service, source_id, dest_id, banned_data):
    page_token = None
    while True:
        try:
            response = execute_with_retry(service.files().list, q=f"'{source_id}' in parents", fields='nextPageToken, files(id, name, mimeType, size)', pageSize=CHUNK_SIZE, pageToken=page_token)
            files_list = response.get('files', [])
            for item in files_list:
                new_name = apply_rename_rules(item['name'], banned_data['rename_rules'])
                if should_skip_item(new_name, item['mimeType'], item.get('size', 0), banned_data): continue
                if item['mimeType'] == 'application/vnd.google-apps.folder':
                    logger.info(f"Subfolder: {new_name}")
                    new_subfolder = service.files().create(body={'name': new_name, 'parents': [dest_id], 'mimeType': 'application/vnd.google-apps.folder'}).execute()
                    copy_folder_contents(service, item['id'], new_subfolder['id'], banned_data)
                else:
                    service.files().copy(fileId=item['id'], body={'parents': [dest_id]}).execute()
            page_token = response.get('nextPageToken')
            if not page_token: break
        except Exception as e:
            logger.error(f"Error in folder contents {source_id}: {str(e)}")
            break

def rename_files_and_folders(service, folder_id, rename_rules):
    page_token = None
    while True:
        try:
            response = execute_with_retry(service.files().list, q=f"'{folder_id}' in parents", fields='nextPageToken, files(id, name, mimeType)', pageSize=CHUNK_SIZE, pageToken=page_token)
            for item in response.get('files', []):
                try:
                    current_name = item['name']
                    new_name = apply_rename_rules(current_name, rename_rules)
                    at_pattern = re.compile(r'@\w+')
                    at_match = at_pattern.search(new_name)
                    if at_match: new_name = at_pattern.sub('@TechZoneX', new_name)
                    elif item['mimeType'] == 'video/mp4' and new_name.endswith('.mp4'): new_name = new_name.replace('.mp4', ' (Telegram@TechZoneX).mp4')
                    if new_name != current_name: service.files().update(fileId=item['id'], body={'name': new_name}).execute()
                except Exception: continue
            page_token = response.get('nextPageToken')
            if not page_token: break
        except Exception: break

def extract_folder_id(url):
    patterns = [r'/folders/([a-zA-Z0-9-_]+)', r'[?&]id=([a-zA-Z0-9-_]+)', r'/folderview[?&]id=([a-zA-Z0-9-_]+)', r'/mobile/folders/([a-zA-Z0-9-_]+)', r'/mobile/folders/[^/]+/([a-zA-Z0-9-_]+)', r'/drive/u/\d+/mobile/folders/([a-zA-Z0-9-_]+)']
    for pattern in patterns:
        match = re.search(pattern, url)
        if match: return match.group(1)
    return None

def extract_file_id(url):
    patterns = [r'/file/d/([a-zA-Z0-9-_]+)', r'/open\?id=([a-zA-Z0-9-_]+)', r'/uc\?id=([a-zA-Z0-9-_]+)', r'/mobile\?id=([a-zA-Z0-9-_]+)']
    for pattern in patterns:
        match = re.search(pattern, url)
        if match: return match.group(1)
    return None

def adjust_entity_offsets(text, entities):
    if not entities: return []
    utf16_to_char = {}
    char_pos = 0
    utf16_pos = 0
    for char in text:
        utf16_to_char[utf16_pos] = char_pos
        utf16_pos += len(char.encode('utf-16-le')) // 2
        char_pos += 1
    adjusted_entities = []
    for entity in entities:
        start = utf16_to_char.get(entity.offset, entity.offset)
        end = utf16_to_char.get(entity.offset + entity.length, entity.offset + entity.length)
        new_entity = MessageEntity(type=entity.type, offset=start, length=end - start, url=entity.url, user=entity.user, language=entity.language, custom_emoji_id=entity.custom_emoji_id)
        adjusted_entities.append(new_entity)
    return adjusted_entities

def filter_entities(entities):
    allowed_types = {MessageEntity.BOLD, MessageEntity.ITALIC, MessageEntity.CODE, MessageEntity.PRE, MessageEntity.UNDERLINE, MessageEntity.STRIKETHROUGH, MessageEntity.TEXT_LINK, MessageEntity.SPOILER, "blockquote"}
    return [e for e in entities if getattr(e, 'type', None) in allowed_types] if entities else []

def apply_formatting(text, entities):
    """Restored Original Logic with blockquote entity support and removed manual '>'-based blockquote handling"""
    if not text: return text
    chars = list(text)
    text_length = len(chars)
    sorted_entities = sorted(entities or [], key=lambda e: -e.offset)
    entity_tags = {
        MessageEntity.BOLD: ('<b>', '</b>'),
        MessageEntity.ITALIC: ('<i>', '</i>'),
        MessageEntity.UNDERLINE: ('<u>', '</u>'),
        MessageEntity.STRIKETHROUGH: ('<s>', '</s>'),
        MessageEntity.SPOILER: ('<tg-spoiler>', '</tg-spoiler>'),
        MessageEntity.CODE: ('<code>', '</code>'),
        MessageEntity.PRE: ('<pre>', '</pre>'),
        MessageEntity.TEXT_LINK: (lambda e: f'<a href="{e.url}">', '</a>'),
        "blockquote": ('<blockquote>', '</blockquote>')
    }
    for entity in sorted_entities:
        entity_type = getattr(entity, 'type', None)
        if entity_type not in entity_tags: continue
        start_tag, end_tag = entity_tags[entity_type]
        if callable(start_tag): start_tag = start_tag(entity)
        start = entity.offset
        end = start + entity.length
        if start >= text_length or end > text_length: continue
        before = ''.join(chars[:start])
        content = ''.join(chars[start:end])
        after = ''.join(chars[end:])
        # Keep inner formatting intact (do not strip tags inside blockquote)
        chars = list(before + start_tag + content + end_tag + after)
        text_length = len(chars)
    
    # NOTE: removed line-based '>' blockquote processing because Telegram blockquote entities
    # are already handled above. Keeping manual '>' handling caused truncation issues.
    
    # Escape everything, then unescape allowed tags
    formatted_text = ''.join(chars)
    formatted_text = formatted_text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    html_tags = ['b', 'i', 'u', 's', 'code', 'pre', 'a', 'tg-spoiler', 'blockquote']
    for tag in html_tags:
        formatted_text = formatted_text.replace(f'&lt;{tag}&gt;', f'<{tag}>').replace(f'&lt;/{tag}&gt;', f'</{tag}>')
    return formatted_text

def close_dangling_tags(html_text):
    """
    Scans HTML and closes any tags that were left open (e.g. by truncation or blockquote logic).
    Also ignores unmatched closing tags to avoid 'unmatched end tag' errors.
    This implementation builds the output progressively and avoids leaving unmatched closers.
    """
    # We'll parse through html_text and reconstruct a balanced version
    tag_regex = re.compile(r'(<!--.*?-->|</?[a-zA-Z][a-zA-Z0-9\-]*(?:\s[^>]*)?>)', re.DOTALL)
    pos = 0
    out_parts = []
    stack = []  # stack of open tag names

    for m in tag_regex.finditer(html_text):
        start, end = m.span()
        # append text before the tag
        before = html_text[pos:start]
        if before:
            out_parts.append(before)
        tag_full = m.group(1)
        pos = end

        # Ignore HTML comments as-is
        if tag_full.startswith('<!--'):
            out_parts.append(tag_full)
            continue

        # Determine if self-closing
        if tag_full.endswith('/>'):
            out_parts.append(tag_full)
            continue

        # Is it a closing tag?
        closing_match = re.match(r'</\s*([a-zA-Z0-9\-]+)\s*>', tag_full)
        if closing_match:
            tag_name = closing_match.group(1)
            if stack and stack[-1] == tag_name:
                # Normal closing for last opened
                stack.pop()
                out_parts.append(tag_full)
            else:
                # Unmatched closer: try to find it in the stack
                if tag_name in stack:
                    # Close intermediate tags first (insert explicit closers), then close this tag
                    temp = []
                    while stack and stack[-1] != tag_name:
                        t = stack.pop()
                        temp.append(t)
                        out_parts.append(f'</{t}>')
                    if stack and stack[-1] == tag_name:
                        stack.pop()
                        out_parts.append(tag_full)
                    # any temp were already closed
                else:
                    # Completely unmatched closing tag -> skip it to avoid breaking structure
                    # (do not append it)
                    continue
        else:
            # Opening tag: capture the name and push to stack and append
            open_match = re.match(r'<\s*([a-zA-Z0-9\-]+)(?:\s[^>]*)?>', tag_full)
            if open_match:
                tag_name = open_match.group(1)
                stack.append(tag_name)
                out_parts.append(tag_full)
            else:
                # malformed tag; append as-is
                out_parts.append(tag_full)

    # append any trailing text after last tag
    tail = html_text[pos:]
    if tail:
        out_parts.append(tail)

    # Close any still-open tags in reverse order
    while stack:
        t = stack.pop()
        out_parts.append(f'</{t}>')

    return ''.join(out_parts)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message or (message.text and message.text.startswith('/')): return
    
    logger.info("New message received.")
    
    # 1. Start with original text and entities
    original_text = message.caption or message.text or ''
    # Convert utf-16 offsets to char offsets immediately
    original_entities = adjust_entity_offsets(original_text, filter_entities(message.caption_entities if message.caption else message.entities))
    
    final_text = original_text
    final_entities = original_entities
    
    drive_links = []

    try:
        drive_service = get_drive_service()
        banned_data = initialize_banned_items(drive_service)

        if original_text:
            url_matches = list(re.finditer(
                r'https?://(?:drive\.google\.com/(?:drive/folders/|folderview\?id=|file/d/|open\?id=|uc\?id=|mobile/folders/|mobile\?id=|.*[?&]id=|drive/u/\d+/mobile/folders/)|.*\.google\.com/open\?id=)[\w-]+[^\s>]*',
                original_text
            ))
            
            logger.info(f"Found {len(url_matches)} Drive links.")
            
            # We process matches in reverse order so replacements don't mess up earlier indices
            # But here we just build a list of replacements
            replacements = []

            for match in url_matches:
                url = match.group()
                logger.info(f"Processing: {url}")
                folder_id = extract_folder_id(url)
                file_id = extract_file_id(url)
                
                new_url = None
                
                if folder_id:
                    try:
                        logger.info(f"Found FOLDER {folder_id}. Starting task...")
                        new_id = await asyncio.get_event_loop().run_in_executor(None, copy_folder, drive_service, folder_id, banned_data)
                        new_url = f'https://drive.google.com/drive/folders/{new_id}'
                    except Exception as e:
                        logger.error(f"Error folder {url}: {str(e)}")
                        await message.reply_text(f"⚠️ Error: {str(e)}")
                        continue
                elif file_id:
                    try:
                        logger.info(f"Found FILE {file_id}. Starting task...")
                        new_id = await asyncio.get_event_loop().run_in_executor(None, copy_file, drive_service, file_id, banned_data)
                        new_url = f'https://drive.google.com/file/d/{new_id}/view?usp=sharing'
                    except Exception as e:
                        logger.error(f"Error file {url}: {str(e)}")
                        await message.reply_text(f"⚠️ Error: {str(e)}")
                        continue

                if new_url:
                    replacements.append((match.start(), match.end(), new_url))
            
            # Apply replacements and update entities manually
            # We sort replacements by start index to process sequentially
            replacements.sort(key=lambda x: x[0])
            
            offset_shift = 0
            
            # Create a new list for modified entities
            current_entities = []
            # We clone existing entities to safe objects
            for e in final_entities:
                current_entities.append(MessageEntity(e.type, e.offset, e.length, url=e.url, user=e.user, language=e.language, custom_emoji_id=e.custom_emoji_id))
            
            # Reconstruct text
            new_text_builder = ""
            last_idx = 0
            
            for start, end, new_str in replacements:
                # Add chunk before link
                new_text_builder += original_text[last_idx:start]
                # Add new link
                new_text_builder += new_str
                
                # Calculate shift for this specific replacement
                diff = len(new_str) - (end - start)
                
                last_idx = end
                
                # Apply shift to entities
                for ent in current_entities:
                    # Entity starts after this link (original position)
                    if ent.offset >= end:
                        ent.offset += diff
                    # Entity covers this link (starts before, ends after)
                    elif ent.offset <= start and (ent.offset + ent.length) >= end:
                        ent.length += diff
                
            new_text_builder += original_text[last_idx:]
            final_text = new_text_builder
            final_entities = current_entities

            # Truncation: Find the LAST replaced link in the final text
            if replacements:
                # The last replacement in the list corresponds to the last link
                # We need to find where that link ended up in final_text
                # Since we rebuilt final_text, we can just search for the URL?
                # Or cleaner: Just find the last occurrence of the last new_url
                last_new_url = replacements[-1][2]
                trunc_pos = final_text.rfind(last_new_url) + len(last_new_url)
                
                if trunc_pos > len(last_new_url):
                     final_text = final_text[:trunc_pos]
                     
                     # Filter entities that are now out of bounds
                     valid_entities = []
                     for ent in final_entities:
                         if ent.offset >= trunc_pos:
                             continue # Remove entity
                         if (ent.offset + ent.length) > trunc_pos:
                             ent.length = trunc_pos - ent.offset # Clamp entity
                         valid_entities.append(ent)
                     final_entities = valid_entities

        # Now apply formatting using the updated text and entities
        formatted_html = apply_formatting(final_text, final_entities)
        
        # FINAL SAFETY NET: Close any tags that got broken
        formatted_html = close_dangling_tags(formatted_html)

        send_args = {
            'chat_id': TARGET_CHANNEL,
            'disable_notification': True,
            'parse_mode': ParseMode.HTML
        }
        
        logger.info(f"Sending to {TARGET_CHANNEL}")

        if message.photo:
            send_args['caption'] = formatted_html
            await context.bot.send_photo(photo=message.photo[-1].file_id, **send_args)
        elif message.video:
            send_args['caption'] = formatted_html
            await context.bot.send_video(video=message.video.file_id, **send_args)
        elif message.document:
            send_args['caption'] = formatted_html
            await context.bot.send_document(document=message.document.file_id, **send_args)
        elif message.audio:
            send_args['caption'] = formatted_html
            await context.bot.send_audio(audio=message.audio.file_id, **send_args)
        else:
            await context.bot.send_message(text=formatted_html, disable_notification=True, chat_id=TARGET_CHANNEL, parse_mode=ParseMode.HTML)
        logger.info("Sent successfully.")

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"⚠️ Error: {str(e)[:200]}")

async def ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not context.args:
            await update.message.reply_text("❌ Usage: /ban <link>")
            return
        input_text = ' '.join(context.args).strip()
        drive_service = get_drive_service()
        banned_data = initialize_banned_items(drive_service)
        
        file_id = extract_file_id(input_text)
        folder_id = extract_folder_id(input_text) if not file_id else None
        
        if file_id or folder_id:
            item_id = file_id or folder_id
            item_info = execute_with_retry(drive_service.files().get, fileId=item_id, fields='name,size,mimeType')
            size_type_str = f"{item_info.get('size', '0')}:{item_info.get('mimeType', 'unknown')}"
            
            if size_type_str not in banned_data['size_types']:
                banned_data['size_types'].append(size_type_str)
                save_banned_items(drive_service, banned_data)
                await update.message.reply_text(f"✅ Banned type: {size_type_str}")
            else:
                await update.message.reply_text("⚠️ Already banned.")
        else:
            if input_text not in banned_data['names']:
                banned_data['names'].append(input_text)
                save_banned_items(drive_service, banned_data)
                await update.message.reply_text(f"✅ Banned name: {input_text}")
            else:
                await update.message.reply_text("⚠️ Already banned.")
    except Exception as e:
        await update.message.reply_text(f"⚠️ Error: {str(e)}")

async def unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not context.args: return
        input_text = ' '.join(context.args).strip()
        drive_service = get_drive_service()
        banned_data = initialize_banned_items(drive_service)
        
        file_id = extract_file_id(input_text)
        folder_id = extract_folder_id(input_text) if not file_id else None
        
        if file_id or folder_id:
            item_id = file_id or folder_id
            item_info = execute_with_retry(drive_service.files().get, fileId=item_id, fields='name,size,mimeType')
            size_type_str = f"{item_info.get('size', '0')}:{item_info.get('mimeType', 'unknown')}"
            if size_type_str in banned_data['size_types']:
                banned_data['size_types'].remove(size_type_str)
                save_banned_items(drive_service, banned_data)
                await update.message.reply_text("✅ Unbanned.")
        else:
            if input_text in banned_data['names']:
                banned_data['names'].remove(input_text)
                save_banned_items(drive_service, banned_data)
                await update.message.reply_text("✅ Unbanned.")
    except Exception as e:
        await update.message.reply_text(f"⚠️ Error: {str(e)}")

async def change(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not context.args: return
        args = ' '.join(context.args).split(' to ', 1)
        if len(args) != 2: return
        drive_service = get_drive_service()
        banned_data = initialize_banned_items(drive_service)
        rename_rule = f"{args[0].strip()}|{args[1].strip()}"
        if rename_rule not in banned_data['rename_rules']:
            banned_data['rename_rules'].append(rename_rule)
            save_banned_items(drive_service, banned_data)
            await update.message.reply_text(f"✅ Added rule.")
    except Exception as e:
        await update.message.reply_text(f"⚠️ Error: {str(e)}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🚀 Bot Ready.\n/auth\n/ban\n/unban\n/change")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Exception: {context.error}")

async def shutdown(signal, loop):
    logger.info("Shutting down...")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

async def run_bot():
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("ban", ban))
    application.add_handler(CommandHandler("unban", unban))
    application.add_handler(CommandHandler("change", change))
    
    auth_conv = ConversationHandler(
        entry_points=[CommandHandler("auth", auth_command)],
        states={AUTH_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_auth_code)]},
        fallbacks=[CommandHandler("cancel", cancel_auth)]
    )
    application.add_handler(auth_conv)
    application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_message))
    application.add_error_handler(error_handler)
    
    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    
    await run_webserver()
    asyncio.create_task(self_ping())
    
    while True:
        await asyncio.sleep(3600)

async def main():
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(shutdown(signal.SIGINT, loop)))
    loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(shutdown(signal.SIGTERM, loop)))
    
    try:
        await run_bot()
    except Exception as e:
        logger.error(f"Fatal: {str(e)}")
    finally:
        global runner, site
        if site: await site.stop()
        if runner: await runner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
