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

# ==========================================
# CONFIGURATION
# ==========================================
BOT_TOKEN = "7846379611:AAFk9kkoQwsA6fCS4vF4Ltr6xn1W645nHFM"
TOKEN_PATH = 'token.json'
CREDENTIALS_PATH = 'credentials.json'
PHASE2_SOURCE = '1ixJU6s6bKbzIdsbjKDKrZYLt1nl_TSul'
PHASE3_SOURCE = '1iM6ghIcYsx1gIvfdjm-HjCRW3MWy0JCP'
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
AUTH_STATE = 1

# Global variables
pending_authorizations = {}
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

# ==========================================
# GOOGLE DRIVE & AUTH FUNCTIONS
# ==========================================

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

# ==========================================
# DRIVE OPERATIONS
# ==========================================

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
            if e.resp.status in [403, 429, 500, 502, 503, 504]:
                logger.warning(f"⚠️ API Error {e.resp.status} in {func_name}. Waiting {wait_time}s...")
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
            raise Exception(f"Operation {func_name} failed after {MAX_RETRIES} attempts.")
    raise Exception(f"Operation {func_name} failed.")

def copy_file(service, file_id, banned_data):
    logger.info(f"STEP: Processing single file {file_id}")
    file = execute_with_retry(service.files().get, fileId=file_id, fields='name,mimeType,size')
    new_name = apply_rename_rules(file['name'], banned_data['rename_rules'])
    if should_skip_item(new_name, file['mimeType'], file.get('size', 0), banned_data):
        raise Exception(f"File {new_name} is banned")
    copied_file = service.files().copy(fileId=file_id).execute()
    return copied_file['id']

def copy_folder(service, folder_id, banned_data):
    logger.info(f"STEP: Starting folder copy: {folder_id}")
    folder = execute_with_retry(service.files().get, fileId=folder_id, fields='name')
    new_folder_name = apply_rename_rules(folder['name'], banned_data['rename_rules'])
    new_folder = service.files().create(body={'name': new_folder_name, 'mimeType': 'application/vnd.google-apps.folder'}).execute()
    new_folder_id = new_folder['id']

    copy_folder_contents(service, folder_id, new_folder_id, banned_data)
    subfolders = get_all_subfolders_recursive(service, new_folder_id)
    
    for i, subfolder_id in enumerate(subfolders):
        copy_files_only(service, PHASE2_SOURCE, subfolder_id, banned_data, overwrite=True)
        if i % 10 == 0: time.sleep(1)

    copy_bonus_content(service, PHASE3_SOURCE, new_folder_id, banned_data, overwrite=True)
    
    rename_files_and_folders(service, new_folder_id, banned_data['rename_rules'])
    for i, subfolder_id in enumerate(subfolders):
        rename_files_and_folders(service, subfolder_id, banned_data['rename_rules'])
        if i % 10 == 0: time.sleep(1)
    return new_folder_id

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
            except Exception: break
    return subfolders

def copy_files_only(service, source_id, dest_id, banned_data, overwrite=False):
    copy_bonus_content(service, source_id, dest_id, banned_data, overwrite)

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
            for item in response.get('files', []):
                new_name = apply_rename_rules(item['name'], banned_data['rename_rules'])
                if should_skip_item(new_name, item['mimeType'], item.get('size', 0), banned_data): continue
                if item['mimeType'] == 'application/vnd.google-apps.folder':
                    new_subfolder = service.files().create(body={'name': new_name, 'parents': [dest_id], 'mimeType': 'application/vnd.google-apps.folder'}).execute()
                    copy_folder_contents(service, item['id'], new_subfolder['id'], banned_data)
                else:
                    service.files().copy(fileId=item['id'], body={'parents': [dest_id]}).execute()
            page_token = response.get('nextPageToken')
            if not page_token: break
        except Exception: break

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

# ==========================================
# STRICTLY IMPORTED FORMATTING LOGIC
# ==========================================

def adjust_entity_offsets(text, entities):
    """Convert UTF-16 entity offsets to character offsets"""
    if not entities:
        return []
    
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
        
        new_entity = MessageEntity(
            type=entity.type,
            offset=start,
            length=end - start,
            url=entity.url,
            user=entity.user,
            language=entity.language,
            custom_emoji_id=entity.custom_emoji_id
        )
        adjusted_entities.append(new_entity)
    
    return adjusted_entities

def filter_entities(entities):
    """Filter only allowed entity types"""
    allowed_types = {
        MessageEntity.BOLD,
        MessageEntity.ITALIC,
        MessageEntity.CODE,
        MessageEntity.PRE,
        MessageEntity.UNDERLINE,
        MessageEntity.STRIKETHROUGH,
        MessageEntity.TEXT_LINK,
        MessageEntity.SPOILER,
        "blockquote"
    }
    
    if not entities:
        return []
    
    return [e for e in entities if getattr(e, 'type', None) in allowed_types]

def apply_formatting_simple(text, entities, skip_types=None):
    """Simple formatting without special blockquote handling"""
    if not entities:
        return text
    
    if skip_types is None:
        skip_types = set()
    
    # Escape HTML first
    formatted_text = text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    
    # Apply entities in reverse order
    sorted_entities = sorted(entities, key=lambda e: -e.offset)
    
    entity_tags = {
        MessageEntity.BOLD: ('<b>', '</b>'),
        MessageEntity.ITALIC: ('<i>', '</i>'),
        MessageEntity.UNDERLINE: ('<u>', '</u>'),
        MessageEntity.STRIKETHROUGH: ('<s>', '</s>'),
        MessageEntity.SPOILER: ('<tg-spoiler>', '</tg-spoiler>'),
        MessageEntity.CODE: ('<code>', '</code>'),
        MessageEntity.PRE: ('<pre>', '</pre>'),
        MessageEntity.TEXT_LINK: (lambda e: f'<a href="{e.url}">', '</a>'),
    }
    
    for entity in sorted_entities:
        entity_type = getattr(entity, 'type', None)
        if entity_type not in entity_tags or entity_type in skip_types:
            continue
        
        start_tag, end_tag = entity_tags[entity_type]
        if callable(start_tag):
            start_tag = start_tag(entity)
        
        start = entity.offset
        end = start + entity.length
        
        # Insert tags
        formatted_text = formatted_text[:start] + start_tag + formatted_text[start:end] + end_tag + formatted_text[end:]
    
    # Restore HTML tags
    html_tags = ['b', 'i', 'u', 's', 'code', 'pre', 'a', 'tg-spoiler']
    for tag in html_tags:
        formatted_text = formatted_text.replace(f'&lt;{tag}&gt;', f'<{tag}>').replace(f'&lt;/{tag}&gt;', f'</{tag}>')
    
    return formatted_text

def apply_formatting(text, entities):
    """Apply formatting to text based on entities - FIXED VERSION"""
    if not text:
        return text
    
    # Make a copy of entities
    if entities:
        entities = [MessageEntity(
            type=e.type,
            offset=e.offset,
            length=e.length,
            url=e.url,
            user=e.user,
            language=e.language,
            custom_emoji_id=e.custom_emoji_id
        ) for e in entities]
    
    # Sort entities by offset (ascending) and length (descending)
    sorted_entities = sorted(entities or [], key=lambda e: (e.offset, -e.length))
    
    segments = []
    last_end = 0
    
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
    
    i = 0
    while i < len(sorted_entities):
        entity = sorted_entities[i]
        entity_type = getattr(entity, 'type', None)
        
        if entity_type not in entity_tags:
            i += 1
            continue
        
        if entity.offset > last_end:
            segments.append(text[last_end:entity.offset])
        
        entity_end = entity.offset + entity.length
        
        # Find nested entities
        nested_entities = []
        j = i + 1
        while j < len(sorted_entities):
            next_entity = sorted_entities[j]
            if (next_entity.offset >= entity.offset and 
                next_entity.offset + next_entity.length <= entity_end):
                adjusted_entity = MessageEntity(
                    type=next_entity.type,
                    offset=next_entity.offset - entity.offset,
                    length=next_entity.length,
                    url=next_entity.url,
                    user=next_entity.user,
                    language=next_entity.language,
                    custom_emoji_id=next_entity.custom_emoji_id
                )
                nested_entities.append(adjusted_entity)
                j += 1
            else:
                break
        
        entity_content = text[entity.offset:entity_end]
        
        # Apply formatting to nested content
        if nested_entities:
            # THIS IS THE CRITICAL LOGIC FROM SCRIPT 2
            formatted_content = apply_formatting_simple(entity_content, nested_entities)
        else:
            formatted_content = entity_content
        
        start_tag, end_tag = entity_tags[entity_type]
        if callable(start_tag):
            start_tag = start_tag(entity)
        
        segments.append(start_tag)
        segments.append(formatted_content)
        segments.append(end_tag)
        
        i += 1 + len(nested_entities)
        last_end = entity_end
    
    if last_end < len(text):
        segments.append(text[last_end:])
    
    formatted_text = ''.join(segments)
    
    # Handle manual blockquote detection
    if ">" in formatted_text:
        formatted_text = formatted_text.replace("&gt;", ">")
        lines = formatted_text.split('\n')
        formatted_lines = []
        in_blockquote = False
        
        for line in lines:
            stripped_line = line.lstrip()
            if stripped_line.startswith('>'):
                if not in_blockquote:
                    formatted_lines.append('<blockquote>')
                    in_blockquote = True
                content_line = stripped_line[1:].lstrip()
                formatted_lines.append(content_line)
            else:
                if in_blockquote:
                    formatted_lines.append('</blockquote>')
                    in_blockquote = False
                formatted_lines.append(line)
        
        if in_blockquote:
            formatted_lines.append('</blockquote>')
        
        formatted_text = '\n'.join(formatted_lines)
    
    formatted_text = formatted_text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    
    html_tags = ['b', 'i', 'u', 's', 'code', 'pre', 'a', 'tg-spoiler', 'blockquote']
    for tag in html_tags:
        formatted_text = formatted_text.replace(f'&lt;{tag}&gt;', f'<{tag}>').replace(f'&lt;/{tag}&gt;', f'</{tag}>')
    
    return formatted_text

def close_dangling_tags(html_text):
    """Close any unclosed HTML tags"""
    tags_stack = []
    matches = list(re.finditer(r'</?([a-z-]+)[^>]*>', html_text))
    
    for m in matches:
        tag_full = m.group(0)
        tag_name = m.group(1)
        
        if tag_full.startswith('</'):
            if tags_stack and tags_stack[-1] == tag_name:
                tags_stack.pop()
            else:
                html_text = html_text[:m.start()] + html_text[m.end():]
                matches = list(re.finditer(r'</?([a-z-]+)[^>]*>', html_text))
        elif not tag_full.endswith('/>'):
            tags_stack.append(tag_name)
    
    closing_tags = ''.join([f'</{tag}>' for tag in reversed(tags_stack)])
    return html_text + closing_tags

# ==========================================
# MAIN BOT LOGIC
# ==========================================

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message or (message.text and message.text.startswith('/')): return
    
    logger.info("New message received.")
    
    # 1. Get original text and adjust entity offsets immediately using correct function
    original_text = message.caption or message.text or ''
    original_entities = adjust_entity_offsets(
        original_text, 
        filter_entities(message.caption_entities if message.caption else message.entities)
    )
    
    # Default to returning original if no links found
    final_text = original_text
    final_entities = original_entities
    processed_any_link = False

    try:
        drive_service = get_drive_service()
        banned_data = initialize_banned_items(drive_service)

        if original_text:
            url_matches = list(re.finditer(
                r'https?://(?:drive\.google\.com/(?:drive/folders/|folderview\?id=|file/d/|open\?id=|uc\?id=|mobile/folders/|mobile\?id=|.*[?&]id=|drive/u/\d+/mobile/folders/)|.*\.google\.com/open\?id=)[\w-]+[^\s>]*',
                original_text
            ))
            
            logger.info(f"Found {len(url_matches)} Drive links.")
            
            # Since user says the link is the "last thing", we iterate in reverse
            # and stop as soon as we successfully process one link.
            for match in reversed(url_matches):
                url = match.group()
                logger.info(f"Processing candidate: {url}")
                folder_id = extract_folder_id(url)
                file_id = extract_file_id(url)
                
                new_url = None
                
                try:
                    if folder_id:
                        logger.info(f"Found FOLDER {folder_id}. Starting task...")
                        new_id = await asyncio.get_event_loop().run_in_executor(None, copy_folder, drive_service, folder_id, banned_data)
                        new_url = f'https://drive.google.com/drive/folders/{new_id}'
                    elif file_id:
                        logger.info(f"Found FILE {file_id}. Starting task...")
                        new_id = await asyncio.get_event_loop().run_in_executor(None, copy_file, drive_service, file_id, banned_data)
                        new_url = f'https://drive.google.com/file/d/{new_id}/view?usp=sharing'
                except Exception as e:
                    logger.error(f"Error processing {url}: {str(e)}")
                    continue # Try next link if this one fails

                if new_url:
                    # SIMPLIFIED LOGIC:
                    # 1. Take everything BEFORE the link match.
                    # 2. Add the new link.
                    # 3. Discard everything after.
                    start_index = match.start()
                    final_text = original_text[:start_index] + new_url
                    
                    # 4. Filter entities.
                    # Since we didn't change the length of the text BEFORE the link,
                    # the offsets of all entities in that region are perfectly preserved.
                    # We just drop any entity that starts after the cut-off.
                    final_entities = [e for e in original_entities if e.offset + e.length <= start_index]
                    
                    processed_any_link = True
                    break # Stop after processing the last link

        # STRICTLY USE THE FORMATTING LOGIC FROM SCRIPT 2
        formatted_html = apply_formatting(final_text, final_entities)
        formatted_html = close_dangling_tags(formatted_html)

        send_args = {
            'chat_id': TARGET_CHANNEL,
            'disable_notification': True,
            'parse_mode': ParseMode.HTML
        }
        
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
        logger.error(traceback.format_exc())
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"⚠️ Error: {str(e)[:200]}")

# ==========================================
# COMMAND HANDLERS
# ==========================================

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

# ==========================================
# SERVER & MAIN LOOP
# ==========================================

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


