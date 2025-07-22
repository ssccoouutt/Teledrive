#!/usr/bin/env python3
"""
Google Drive Content Processor Bot for Telegram
Automatically processes Google Drive links, copies content, adds bonus materials, and forwards to channel
"""

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
from typing import Dict, List, Tuple, Optional
from aiohttp import web
from telegram import Update, MessageEntity
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ConversationHandler
)
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow

# ==================== CONFIGURATION ====================
BOT_TOKEN = "7846379611:AAHYshaf3fYSh44JXQEnfttrggq4-OrP5AQ"
TOKEN_PATH = 'token.json'
CREDENTIALS_PATH = 'credentials.json'
PHASE2_SOURCE = '1ixJU6s6bKbzIdsbjKDKrZYLt1nl_TSul'
PHASE3_SOURCE = '1iM6ghIcYsx1gIvfdjm-HjCRW3MWy0JCP'
SHORT_LINKS = ["rb.gy/cd8ugy", "bit.ly/3UcvhlA", "t.ly/CfcVB", "cutt.ly/Kee3oiLO"]
TARGET_CHANNEL = "@techworld196"
BANNED_FILE_ID = '1r2BpwG9isOkKjL5tYj3WqqiF5w4oWpCY'
SCOPES = ['https://www.googleapis.com/auth/drive']

# Web Server Configuration
WEB_PORT = 8000
PING_INTERVAL = 25
HEALTH_CHECK_ENDPOINT = "/health"

# Operation Constants
MAX_RETRIES = 5
RETRY_DELAY = 15  # seconds
CHUNK_SIZE = 20  # Number of files to process at once
OPERATION_TIMEOUT = 300  # seconds for each operation
MAX_MESSAGE_LENGTH = 4096  # Telegram message limit

# Authorization state
AUTH_STATE = 1
pending_authorizations: Dict[int, InstalledAppFlow] = {}

# ==================== LOGGING SETUP ====================
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('drive_bot.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ==================== EXCEPTIONS ====================
class DriveOperationError(Exception):
    """Base exception for Drive operations"""
    pass

class DriveOperationTimeout(DriveOperationError):
    """Custom exception for Drive operation timeouts"""
    pass

class AuthorizationError(DriveOperationError):
    """Exception for authorization failures"""
    pass

# ==================== UTILITY FUNCTIONS ====================
def extract_folder_id(url: str) -> Optional[str]:
    """Extract folder ID from Google Drive URL with multiple pattern support"""
    patterns = [
        r'/folders/([a-zA-Z0-9-_]+)',  # Standard folder link
        r'[?&]id=([a-zA-Z0-9-_]+)',     # ID parameter links
        r'/folderview[?&]id=([a-zA-Z0-9-_]+)',  # Folderview links
        r'/mobile/folders/([a-zA-Z0-9-_]+)',  # Mobile folder links
        r'/mobile/folders/[^/]+/([a-zA-Z0-9-_]+)',  # Nested mobile folder links
        r'/drive/u/\d+/mobile/folders/([a-zA-Z0-9-_]+)'  # Mobile folder links with user number
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None

def extract_file_id(url: str) -> Optional[str]:
    """Extract file ID from Google Drive URL with multiple pattern support"""
    patterns = [
        r'/file/d/([a-zA-Z0-9-_]+)',
        r'/open\?id=([a-zA-Z0-9-_]+)',
        r'/uc\?id=([a-zA-Z0-9-_]+)',
        r'/mobile\?id=([a-zA-Z0-9-_]+)'
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None

async def timeout_wrapper(coro, timeout: int = OPERATION_TIMEOUT):
    """Wrapper to add timeout to coroutines"""
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        raise DriveOperationTimeout(f"Operation timed out after {timeout} seconds")

# ==================== GOOGLE DRIVE OPERATIONS ====================
def get_drive_service() -> build:
    """Initialize and return Google Drive service with retry"""
    for attempt in range(MAX_RETRIES):
        try:
            creds = None
            if os.path.exists(TOKEN_PATH):
                creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
            
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                    with open(TOKEN_PATH, 'w') as token:
                        token.write(creds.to_json())
                else:
                    raise AuthorizationError('Google Drive authorization required. Use /auth to authenticate.')
            
            return build('drive', 'v3', credentials=creds, cache_discovery=False)
            
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                logger.error("Failed to initialize Drive service after retries")
                raise
            logger.warning(f"Drive service init attempt {attempt + 1} failed, retrying...")
            time.sleep(RETRY_DELAY)

def execute_with_retry(func, *args, **kwargs):
    """Execute a function with retry and timeout handling"""
    last_exception = None
    for attempt in range(MAX_RETRIES):
        try:
            logger.debug(f"Attempt {attempt + 1} for {func.__name__}")
            result = func(*args, **kwargs).execute()
            return result
        except HttpError as e:
            if e.resp.status in [500, 502, 503, 504]:
                last_exception = e
                logger.warning(f"Attempt {attempt + 1} failed with status {e.resp.status}, retrying...")
                time.sleep(RETRY_DELAY)
                continue
            raise
        except Exception as e:
            last_exception = e
            if attempt < MAX_RETRIES - 1:
                logger.warning(f"Attempt {attempt + 1} failed, retrying...")
                time.sleep(RETRY_DELAY)
                continue
            raise
    
    raise DriveOperationError(f"Operation failed after {MAX_RETRIES} attempts. Last error: {str(last_exception)}")

def initialize_banned_items(service: build) -> Dict[str, List[str]]:
    """Load banned items list from Google Drive with retry"""
    for attempt in range(MAX_RETRIES):
        try:
            request = service.files().get_media(fileId=BANNED_FILE_ID)
            banned_file = request.execute().decode('utf-8')
            
            banned_data = {
                'names': [],
                'size_types': [],
                'rename_rules': []
            }
            
            for section in banned_file.split('\n\n'):
                if section.startswith('#BANNED_NAMES'):
                    banned_data['names'] = [line.strip() for line in section.split('\n')[1:] if line.strip()]
                elif section.startswith('#BANNED_SIZE_TYPE'):
                    banned_data['size_types'] = [line.strip() for line in section.split('\n')[1:] if line.strip()]
                elif section.startswith('#RENAME_RULES'):
                    banned_data['rename_rules'] = [line.strip() for line in section.split('\n')[1:] if line.strip()]
            
            logger.info(f"Loaded {len(banned_data['names'])} banned names, {len(banned_data['size_types'])} size-types, {len(banned_data['rename_rules'])} rename rules")
            return banned_data
            
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                logger.error(f"Failed to load banned items after {MAX_RETRIES} attempts")
                return {'names': [], 'size_types': [], 'rename_rules': []}
            logger.warning(f"Banned items load attempt {attempt + 1} failed, retrying...")
            time.sleep(RETRY_DELAY)

def save_banned_items(service: build, banned_data: Dict[str, List[str]]) -> None:
    """Save banned items list to Google Drive"""
    try:
        content = ""
        
        if banned_data['names']:
            content += "#BANNED_NAMES\n" + "\n".join(banned_data['names']) + "\n\n"
        
        if banned_data['size_types']:
            content += "#BANNED_SIZE_TYPE\n" + "\n".join(banned_data['size_types']) + "\n\n"
        
        if banned_data['rename_rules']:
            content += "#RENAME_RULES\n" + "\n".join(banned_data['rename_rules'])
        
        content = content.strip()
        
        media = MediaIoBaseUpload(io.BytesIO(content.encode('utf-8')), mimetype='text/plain')
        execute_with_retry(service.files().update, fileId=BANNED_FILE_ID, media_body=media)
        logger.info("Saved banned items to Google Drive")
    except Exception as e:
        logger.error(f"Error saving banned items: {str(e)}")
        raise

def should_skip_item(name: str, mime_type: str, size: int, banned_data: Dict[str, List[str]]) -> bool:
    """Check if item should be skipped based on banned list"""
    if name in banned_data['names']:
        logger.debug(f"Skipping banned name: {name}")
        return True
    
    size_type_str = f"{size}:{mime_type}"
    if size_type_str in banned_data['size_types']:
        logger.debug(f"Skipping banned size+type: {size_type_str}")
        return True
    
    return False

def apply_rename_rules(name: str, rename_rules: List[str]) -> str:
    """Apply rename rules to a filename"""
    original_name = name
    for rule in rename_rules:
        if '|' in rule:
            old, new = rule.split('|', 1)
            name = name.replace(old, new)
    
    if name != original_name:
        logger.debug(f"Renamed '{original_name}' to '{name}'")
    
    return name

# ==================== CONTENT PROCESSING ====================
async def copy_file(service: build, file_id: str, banned_data: Dict[str, List[str]]) -> str:
    """Copy a single file with retry mechanism"""
    try:
        file = execute_with_retry(service.files().get, fileId=file_id, fields='name,mimeType,size')
        new_name = apply_rename_rules(file['name'], banned_data['rename_rules'])
        
        if should_skip_item(new_name, file['mimeType'], file.get('size', 0), banned_data):
            raise DriveOperationError(f"File {new_name} is banned")
            
        copied_file = execute_with_retry(service.files().copy, fileId=file_id)
        return copied_file['id']
    except Exception as e:
        logger.error(f"File copy failed: {str(e)}")
        raise DriveOperationError(f"File copy failed: {str(e)}")

async def copy_folder_contents(service: build, source_id: str, dest_id: str, banned_data: Dict[str, List[str]]) -> Tuple[int, int]:
    """Copy folder contents with robust error handling"""
    page_token = None
    total_files = 0
    total_folders = 0
    
    while True:
        try:
            response = execute_with_retry(
                service.files().list,
                q=f"'{source_id}' in parents",
                fields='nextPageToken, files(id, name, mimeType, size)',
                pageSize=CHUNK_SIZE,
                pageToken=page_token
            )
            
            items = response.get('files', [])
            if not items:
                break
                
            for item in items:
                try:
                    new_name = apply_rename_rules(item['name'], banned_data['rename_rules'])
                    if should_skip_item(new_name, item['mimeType'], item.get('size', 0), banned_data):
                        continue
                        
                    if item['mimeType'] == 'application/vnd.google-apps.folder':
                        new_folder = execute_with_retry(
                            service.files().create,
                            body={
                                'name': new_name,
                                'parents': [dest_id],
                                'mimeType': 'application/vnd.google-apps.folder'
                            }
                        )
                        await copy_folder_contents(service, item['id'], new_folder['id'], banned_data)
                        total_folders += 1
                    else:
                        execute_with_retry(
                            service.files().copy,
                            fileId=item['id'],
                            body={'parents': [dest_id]}
                        )
                        total_files += 1
                        
                except Exception as e:
                    logger.error(f"Error copying item {item.get('name')}: {str(e)}")
                    continue
                    
            page_token = response.get('nextPageToken')
            if not page_token:
                break
                
        except Exception as e:
            logger.error(f"Error listing folder contents: {str(e)}")
            break
            
    return total_files, total_folders

async def copy_bonus_content(service: build, source_id: str, dest_id: str, banned_data: Dict[str, List[str]]) -> int:
    """Copy bonus content to destination folder"""
    page_token = None
    total_copied = 0
    
    while True:
        try:
            response = execute_with_retry(
                service.files().list,
                q=f"'{source_id}' in parents",
                fields='nextPageToken, files(id, name, mimeType, size)',
                pageSize=CHUNK_SIZE,
                pageToken=page_token
            )
            
            items = response.get('files', [])
            if not items:
                break
                
            for item in items:
                try:
                    new_name = apply_rename_rules(item['name'], banned_data['rename_rules'])
                    if should_skip_item(new_name, item['mimeType'], item.get('size', 0), banned_data):
                        continue
                        
                    if item['mimeType'] == 'application/vnd.google-apps.folder':
                        new_folder = execute_with_retry(
                            service.files().create,
                            body={
                                'name': new_name,
                                'parents': [dest_id],
                                'mimeType': 'application/vnd.google-apps.folder'
                            }
                        )
                        await copy_bonus_content(service, item['id'], new_folder['id'], banned_data)
                    else:
                        execute_with_retry(
                            service.files().copy,
                            fileId=item['id'],
                            body={'parents': [dest_id]}
                        )
                    total_copied += 1
                except Exception as e:
                    logger.error(f"Error copying bonus item {item.get('name')}: {str(e)}")
                    continue
                    
            page_token = response.get('nextPageToken')
            if not page_token:
                break
                
        except Exception as e:
            logger.error(f"Error listing bonus content: {str(e)}")
            break
            
    return total_copied

async def rename_files_and_folders(service: build, folder_id: str, rename_rules: List[str]) -> int:
    """Rename files and folders according to rules"""
    page_token = None
    total_renamed = 0
    
    while True:
        try:
            response = execute_with_retry(
                service.files().list,
                q=f"'{folder_id}' in parents",
                fields='nextPageToken, files(id, name, mimeType)',
                pageSize=CHUNK_SIZE,
                pageToken=page_token
            )
            
            items = response.get('files', [])
            if not items:
                break
                
            for item in items:
                try:
                    current_name = item['name']
                    new_name = apply_rename_rules(current_name, rename_rules)
                    
                    # Apply standard patterns
                    at_pattern = re.compile(r'@\w+')
                    at_match = at_pattern.search(new_name)
                    
                    if at_match:
                        new_name = at_pattern.sub('@TechZoneX', new_name)
                    elif item['mimeType'] == 'video/mp4' and new_name.endswith('.mp4'):
                        new_name = new_name.replace('.mp4', ' (Telegram@TechZoneX).mp4')
                    
                    if new_name != current_name:
                        execute_with_retry(
                            service.files().update,
                            fileId=item['id'],
                            body={'name': new_name}
                        )
                        total_renamed += 1
                except Exception as e:
                    logger.error(f"Error renaming {item.get('name')}: {str(e)}")
                    continue
                    
            page_token = response.get('nextPageToken')
            if not page_token:
                break
                
        except Exception as e:
            logger.error(f"Error listing files for renaming: {str(e)}")
            break
            
    return total_renamed

async def process_folder(service: build, folder_id: str, banned_data: Dict[str, List[str]]) -> str:
    """Complete folder processing with all steps"""
    try:
        # Step 1: Get folder info and create new folder
        folder_info = execute_with_retry(service.files().get, fileId=folder_id, fields='name')
        new_folder_name = apply_rename_rules(folder_info['name'], banned_data['rename_rules'])
        
        new_folder = execute_with_retry(
            service.files().create,
            body={
                'name': new_folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
        )
        new_folder_id = new_folder['id']
        
        logger.info(f"Created new folder: {new_folder_name} ({new_folder_id})")
        
        # Step 2: Copy contents
        files_copied, folders_copied = await copy_folder_contents(service, folder_id, new_folder_id, banned_data)
        logger.info(f"Copied {files_copied} files and {folders_copied} folders")
        
        # Step 3: Get all subfolders
        subfolders = []
        queue = [new_folder_id]
        
        while queue:
            current_id = queue.pop(0)
            page_token = None
            
            while True:
                try:
                    response = execute_with_retry(
                        service.files().list,
                        q=f"'{current_id}' in parents and mimeType='application/vnd.google-apps.folder'",
                        fields='nextPageToken, files(id)',
                        pageSize=CHUNK_SIZE
                    )
                    
                    for folder in response.get('files', []):
                        subfolders.append(folder['id'])
                        queue.append(folder['id'])
                    
                    page_token = response.get('nextPageToken')
                    if not page_token:
                        break
                        
                except Exception as e:
                    logger.error(f"Error getting subfolders: {str(e)}")
                    break
        
        logger.info(f"Found {len(subfolders)} subfolders")
        
        # Step 4: Add phase2 content to subfolders
        phase2_added = 0
        for subfolder_id in subfolders:
            try:
                added = await copy_bonus_content(service, PHASE2_SOURCE, subfolder_id, banned_data)
                phase2_added += added
            except Exception as e:
                logger.error(f"Error adding phase2 content to {subfolder_id}: {str(e)}")
                continue
                
        logger.info(f"Added phase2 content to {len(subfolders)} subfolders ({phase2_added} items)")
        
        # Step 5: Add phase3 content to main folder
        try:
            phase3_added = await copy_bonus_content(service, PHASE3_SOURCE, new_folder_id, banned_data)
            logger.info(f"Added {phase3_added} phase3 items to main folder")
        except Exception as e:
            logger.error(f"Error adding phase3 content: {str(e)}")
            
        # Step 6: Rename files
        renamed_count = await rename_files_and_folders(service, new_folder_id, banned_data['rename_rules'])
        logger.info(f"Renamed {renamed_count} items in main folder")
        
        # Step 7: Rename files in subfolders
        subfolder_renamed = 0
        for subfolder_id in subfolders:
            try:
                count = await rename_files_and_folders(service, subfolder_id, banned_data['rename_rules'])
                subfolder_renamed += count
            except Exception as e:
                logger.error(f"Error renaming files in {subfolder_id}: {str(e)}")
                continue
                
        logger.info(f"Renamed {subfolder_renamed} items in subfolders")
        
        return new_folder_id
        
    except Exception as e:
        logger.error(f"Folder processing failed: {str(e)}")
        raise DriveOperationError(f"Folder processing failed: {str(e)}")

# ==================== MESSAGE PROCESSING ====================
def adjust_entity_offsets(text: str, entities: List[MessageEntity]) -> List[MessageEntity]:
    """Convert UTF-16 based offsets to proper character positions"""
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

def filter_entities(entities: List[MessageEntity]) -> List[MessageEntity]:
    """Filter to only supported formatting entities"""
    allowed_types = {
        MessageEntity.BOLD,
        MessageEntity.ITALIC,
        MessageEntity.CODE,
        MessageEntity.PRE,
        MessageEntity.UNDERLINE,
        MessageEntity.STRIKETHROUGH,
        MessageEntity.TEXT_LINK,
        MessageEntity.SPOILER
    }
    return [e for e in entities if e.type in allowed_types] if entities else []

def apply_formatting(text: str, entities: List[MessageEntity]) -> str:
    """Apply all formatting with proper nesting"""
    if not text:
        return text
    
    # Convert to list for character-level manipulation
    chars = list(text)
    text_length = len(chars)
    
    # Sort entities by offset (reversed for proper insertion)
    sorted_entities = sorted(entities or [], key=lambda e: -e.offset)
    
    # Entity processing map
    entity_tags = {
        MessageEntity.BOLD: ('<b>', '</b>'),
        MessageEntity.ITALIC: ('<i>', '</i>'),
        MessageEntity.UNDERLINE: ('<u>', '</u>'),
        MessageEntity.STRIKETHROUGH: ('<s>', '</s>'),
        MessageEntity.SPOILER: ('<tg-spoiler>', '</tg-spoiler>'),
        MessageEntity.CODE: ('<code>', '</code>'),
        MessageEntity.PRE: ('<pre>', '</pre>'),
        MessageEntity.TEXT_LINK: (lambda e: f'<a href="{e.url}">', '</a>')
    }
    
    for entity in sorted_entities:
        if entity.type not in entity_tags:
            continue
            
        start_tag, end_tag = entity_tags[entity.type]
        if callable(start_tag):
            start_tag = start_tag(entity)
            
        start = entity.offset
        end = start + entity.length
        
        # Validate positions
        if start >= text_length or end > text_length:
            continue
            
        # Apply formatting
        before = ''.join(chars[:start])
        content = ''.join(chars[start:end])
        after = ''.join(chars[end:])
        
        chars = list(before + start_tag + content + end_tag + after)
        text_length = len(chars)
    
    # Handle manual blockquotes (lines starting with >)
    formatted_text = ''.join(chars)
    if ">" in formatted_text:
        formatted_text = formatted_text.replace("&gt;", ">")
        lines = formatted_text.split('\n')
        formatted_lines = []
        in_blockquote = False
        
        for line in lines:
            if line.startswith('>'):
                if not in_blockquote:
                    formatted_lines.append('<blockquote>')
                    in_blockquote = True
                formatted_lines.append(line[1:].strip())
            else:
                if in_blockquote:
                    formatted_lines.append('</blockquote>')
                    in_blockquote = False
                formatted_lines.append(line)
        
        if in_blockquote:
            formatted_lines.append('</blockquote>')
        
        formatted_text = '\n'.join(formatted_lines)
    
    # Final HTML escaping (except for our tags)
    formatted_text = formatted_text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    
    # Re-insert our HTML tags
    html_tags = ['b', 'i', 'u', 's', 'code', 'pre', 'a', 'tg-spoiler', 'blockquote']
    for tag in html_tags:
        formatted_text = formatted_text.replace(f'&lt;{tag}&gt;', f'<{tag}>').replace(f'&lt;/{tag}&gt;', f'</{tag}>')
    
    return formatted_text

async def forward_message_with_updates(
    message: Update.message,
    processed_links: List[Tuple[str, str]],
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Forward message with updated drive links"""
    try:
        original_text = message.caption or message.text or ''
        updated_text = original_text
        
        for old_url, new_url in processed_links:
            updated_text = updated_text.replace(old_url, new_url)
        
        # Apply formatting if needed
        entities = message.caption_entities if message.caption else message.entities
        if entities:
            filtered_entities = filter_entities(entities)
            adjusted_entities = adjust_entity_offsets(updated_text, filtered_entities)
            formatted_text = apply_formatting(updated_text, adjusted_entities)
        else:
            formatted_text = updated_text
        
        send_args = {
            'chat_id': TARGET_CHANNEL,
            'disable_notification': True,
            'parse_mode': ParseMode.HTML
        }
        
        if len(formatted_text) > MAX_MESSAGE_LENGTH:
            formatted_text = formatted_text[:MAX_MESSAGE_LENGTH - 100] + "... [message truncated]"
        
        if message.photo:
            send_args['caption'] = formatted_text
            await context.bot.send_photo(photo=message.photo[-1].file_id, **send_args)
        elif message.video:
            send_args['caption'] = formatted_text
            await context.bot.send_video(video=message.video.file_id, **send_args)
        elif message.document:
            send_args['caption'] = formatted_text
            await context.bot.send_document(document=message.document.file_id, **send_args)
        elif message.audio:
            send_args['caption'] = formatted_text
            await context.bot.send_audio(audio=message.audio.file_id, **send_args)
        else:
            send_args['text'] = formatted_text
            await context.bot.send_message(**send_args)
            
    except Exception as e:
        logger.error(f"Failed to forward message: {str(e)}")
        raise

# ==================== COMMAND HANDLERS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send welcome message with bot instructions"""
    await update.message.reply_text(
        "🚀 TechZoneX Auto Forward Bot\n\n"
        "Send any post with Google Drive links for processing!\n"
        "Commands:\n"
        "/auth - Authorize Google Drive\n"
        "/ban <name_or_link> - Block files/folders\n"
        "/unban <name_or_link> - Unblock files/folders\n"
        "/change <old> to <new> - Add rename rule"
    )

async def auth_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start Google Drive authorization process"""
    try:
        flow = InstalledAppFlow.from_client_secrets_file(
            CREDENTIALS_PATH,
            scopes=SCOPES,
            redirect_uri='http://localhost:8080'
        )
        auth_url, _ = flow.authorization_url(prompt='consent')
        pending_authorizations[update.effective_user.id] = flow
        
        await update.message.reply_text(
            "🔑 *Google Drive Authorization Required*\n\n"
            "1. Click this link to authorize:\n"
            f"[Authorize Google Drive]({auth_url})\n\n"
            "2. After approving, you'll see an error page (This is normal)\n"
            "3. Send me the complete URL from your browser's address bar\n\n"
            "⚠️ *Note:* You may see an 'unverified app' warning. Click 'Advanced' then 'Continue'",
            parse_mode='Markdown',
            disable_web_page_preview=True
        )
        return AUTH_STATE
    except Exception as e:
        logger.error(f"Auth command failed: {str(e)}")
        await update.message.reply_text("⚠️ Authorization setup failed. Please try again.")
        return ConversationHandler.END

async def handle_auth_code(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle received authorization code"""
    user_id = update.effective_user.id
    text = update.message.text.strip()
    
    try:
        # Extract code from URL
        code = None
        if 'code=' in text:
            code = text.split('code=')[1].split('&')[0]
        elif 'localhost' in text and '?code=' in text:
            code = text.split('?code=')[1].split('&')[0]
        
        if not code or user_id not in pending_authorizations:
            await update.message.reply_text("❌ Invalid authorization URL. Please try /auth again")
            return ConversationHandler.END
        
        flow = pending_authorizations[user_id]
        flow.fetch_token(code=code)
        creds = flow.credentials
        
        with open(TOKEN_PATH, 'w') as token_file:
            token_file.write(creds.to_json())
        
        del pending_authorizations[user_id]
        await update.message.reply_text("✅ Authorization successful! Bot is now ready to use.")
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Authorization failed: {str(e)}")
        await update.message.reply_text(f"❌ Authorization failed: {str(e)}")
        return ConversationHandler.END

async def cancel_auth(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancel authorization process"""
    user_id = update.effective_user.id
    if user_id in pending_authorizations:
        del pending_authorizations[user_id]
    
    await update.message.reply_text("❌ Authorization cancelled")
    return ConversationHandler.END

async def ban(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ban a file or folder from being processed"""
    try:
        if not context.args:
            await update.message.reply_text("❌ Usage: /ban <filename_or_folder_or_drive_link>")
            return

        input_text = ' '.join(context.args).strip()
        service = get_drive_service()
        banned_data = initialize_banned_items(service)

        # Check if input is a Google Drive link
        file_id = extract_file_id(input_text)
        folder_id = extract_folder_id(input_text)
        
        if file_id or folder_id:
            # Ban by size and type
            item_id = file_id or folder_id
            item_info = execute_with_retry(service.files().get, 
                fileId=item_id, 
                fields='name,size,mimeType'
            )
            
            size = item_info.get('size', '0')
            mime_type = item_info.get('mimeType', 'unknown')
            size_type_str = f"{size}:{mime_type}"
            
            if size_type_str not in banned_data['size_types']:
                banned_data['size_types'].append(size_type_str)
                save_banned_items(service, banned_data)
                response_text = f"✅ Banned by size+type: {size} bytes, {mime_type}"
            else:
                response_text = f"⚠️ Already banned by size+type: {size} bytes, {mime_type}"
        else:
            # Ban by name
            item_name = input_text
            if item_name not in banned_data['names']:
                banned_data['names'].append(item_name)
                save_banned_items(service, banned_data)
                response_text = f"✅ Banned by name: {item_name}"
            else:
                response_text = f"⚠️ Already banned by name: {item_name}"
        
        await update.message.reply_text(response_text)

    except Exception as e:
        logger.error(f"Ban command failed: {str(e)}")
        await update.message.reply_text(f"⚠️ Ban failed: {str(e)}")

async def unban(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Unban a file or folder"""
    try:
        if not context.args:
            await update.message.reply_text("❌ Usage: /unban <filename_or_folder_or_drive_link>")
            return

        input_text = ' '.join(context.args).strip()
        service = get_drive_service()
        banned_data = initialize_banned_items(service)

        # Check if input is a Google Drive link
        file_id = extract_file_id(input_text)
        folder_id = extract_folder_id(input_text)
        
        if file_id or folder_id:
            # Try to unban by size and type
            item_id = file_id or folder_id
            item_info = execute_with_retry(service.files().get, 
                fileId=item_id, 
                fields='name,size,mimeType'
            )
            
            size = item_info.get('size', '0')
            mime_type = item_info.get('mimeType', 'unknown')
            size_type_str = f"{size}:{mime_type}"
            
            if size_type_str in banned_data['size_types']:
                banned_data['size_types'].remove(size_type_str)
                save_banned_items(service, banned_data)
                response_text = f"✅ Unbanned by size+type: {size} bytes, {mime_type}"
            else:
                response_text = f"⚠️ Not banned by size+type: {size} bytes, {mime_type}"
        else:
            # Try to unban by name
            item_name = input_text
            if item_name in banned_data['names']:
                banned_data['names'].remove(item_name)
                save_banned_items(service, banned_data)
                response_text = f"✅ Unbanned by name: {item_name}"
            else:
                response_text = f"⚠️ Not banned by name: {item_name}"
        
        await update.message.reply_text(response_text)

    except Exception as e:
        logger.error(f"Unban command failed: {str(e)}")
        await update.message.reply_text(f"⚠️ Unban failed: {str(e)}")

async def change(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Add a rename rule for files/folders"""
    try:
        if not context.args or len(context.args) < 3 or context.args[1].lower() != 'to':
            await update.message.reply_text("❌ Usage: /change <old_text> to <new_text>")
            return

        # Split args into old and new parts
        args = ' '.join(context.args).split(' to ', 1)
        if len(args) != 2:
            await update.message.reply_text("❌ Usage: /change <old_text> to <new_text>")
            return

        old_text, new_text = args
        old_text = old_text.strip()
        new_text = new_text.strip()

        if not old_text or not new_text:
            await update.message.reply_text("❌ Both old and new text must be specified")
            return

        service = get_drive_service()
        banned_data = initialize_banned_items(service)

        # Add the rename rule (format: old_text|new_text)
        rename_rule = f"{old_text}|{new_text}"
        if rename_rule not in banned_data['rename_rules']:
            banned_data['rename_rules'].append(rename_rule)
            save_banned_items(service, banned_data)
            response_text = f"✅ Rename rule added: '{old_text}' → '{new_text}'"
        else:
            response_text = f"⚠️ Rename rule already exists: '{old_text}' → '{new_text}'"
        
        await update.message.reply_text(response_text)

    except Exception as e:
        logger.error(f"Change command failed: {str(e)}")
        await update.message.reply_text(f"⚠️ Change command failed: {str(e)}")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle incoming messages with comprehensive error handling"""
    try:
        message = update.message
        if not message:
            return

        text = message.caption or message.text or ''
        if not text:
            return

        # Extract Google Drive links
        drive_links = re.findall(
            r'https?://(?:drive\.google\.com/(?:drive/folders/|folderview\?id=|file/d/|open\?id=|uc\?id=|mobile/folders/|mobile\?id=|.*[?&]id=|drive/u/\d+/mobile/folders/)|.*\.google\.com/open\?id=)[\w-]+[^\s>]*',
            text
        )

        if not drive_links:
            return

        service = get_drive_service()
        banned_data = initialize_banned_items(service)
        
        processed_links = []
        
        for url in drive_links:
            try:
                folder_id = extract_folder_id(url)
                file_id = extract_file_id(url)
                
                if folder_id:
                    logger.info(f"Processing folder: {url}")
                    new_id = await timeout_wrapper(process_folder(service, folder_id, banned_data))
                    new_url = f'https://drive.google.com/drive/folders/{new_id} {random.choice(SHORT_LINKS)}'
                    processed_links.append((url, new_url))
                    
                elif file_id:
                    logger.info(f"Processing file: {url}")
                    new_id = await timeout_wrapper(copy_file(service, file_id, banned_data))
                    new_url = f'https://drive.google.com/file/d/{new_id}/view?usp=sharing {random.choice(SHORT_LINKS)}'
                    processed_links.append((url, new_url))
                    
            except Exception as e:
                logger.error(f"Failed to process {url}: {str(e)}")
                await message.reply_text(f"⚠️ Error processing {url}: {str(e)[:200]}")
                continue
                
        if processed_links:
            await forward_message_with_updates(message, processed_links, context)
            
    except Exception as e:
        logger.error(f"Message handling failed: {str(e)}")
        if update.effective_chat:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"⚠️ Processing error: {str(e)[:200]}"
            )

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle errors in the bot"""
    error = context.error
    tb_list = traceback.format_exception(type(error), error, error.__traceback__)
    tb_string = ''.join(tb_list)
    logger.error(f"Exception occurred:\n{tb_string}")
    
    try:
        if update and update.effective_chat:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="⚠️ An error occurred. Please check the format and try again."
            )
    except Exception as e:
        logger.error(f"Error in error handler while sending message: {e}")

# ==================== WEB SERVER ====================
async def health_check(request: web.Request) -> web.Response:
    """Health check endpoint for Koyeb"""
    return web.Response(
        text=f"🤖 Bot is operational | Last active: {datetime.now()}",
        headers={"Content-Type": "text/plain"},
        status=200
    )

async def run_webserver() -> Tuple[web.AppRunner, web.TCPSite]:
    """Run the web server for health checks"""
    app = web.Application()
    app.router.add_get(HEALTH_CHECK_ENDPOINT, health_check)
    app.router.add_get("/", lambda r: web.Response(text="Bot is running"))
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', WEB_PORT)
    await site.start()
    logger.info(f"Health check server running on port {WEB_PORT}")
    return runner, site

async def self_ping() -> None:
    """Keep-alive mechanism for Koyeb"""
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f'http://localhost:{WEB_PORT}{HEALTH_CHECK_ENDPOINT}') as resp:
                    if resp.status != 200:
                        logger.warning(f"Keepalive ping failed with status {resp.status}")
            
            with open('/tmp/last_active.txt', 'w') as f:
                f.write(str(datetime.now()))
                
        except Exception as e:
            logger.error(f"Keepalive error: {str(e)}")
        
        await asyncio.sleep(PING_INTERVAL)

# ==================== MAIN APPLICATION ====================
async def shutdown(signal, loop):
    """Cleanup tasks tied to the service's shutdown."""
    logger.info(f"Received exit signal {signal.name}...")
    
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    
    logger.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

async def main() -> None:
    """Main application entry point"""
    # Initialize bot application
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("ban", ban))
    application.add_handler(CommandHandler("unban", unban))
    application.add_handler(CommandHandler("change", change))
    
    # Add authorization conversation handler
    auth_conv = ConversationHandler(
        entry_points=[CommandHandler("auth", auth_command)],
        states={
            AUTH_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_auth_code)]
        },
        fallbacks=[CommandHandler("cancel", cancel_auth)]
    )
    application.add_handler(auth_conv)
    
    # Add message handler
    application.add_handler(MessageHandler(
        filters.CAPTION | filters.TEXT | filters.PHOTO |
        filters.VIDEO | filters.Document.ALL | filters.AUDIO &
        ~filters.COMMAND,
        handle_message
    ))
    
    # Add error handler
    application.add_error_handler(error_handler)
    
    # Start components
    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    
    # Start web server and keepalive
    runner, site = await run_webserver()
    asyncio.create_task(self_ping())
    
    # Set up signal handlers
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(sig, loop)))
    
    # Run until shutdown
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        logger.info("Shutdown signal received")
    finally:
        logger.info("Starting cleanup process...")
        await site.stop()
        await runner.cleanup()
        await application.stop()
        await application.shutdown()
        logger.info("Cleanup completed")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}")
        raise
