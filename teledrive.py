from flask import Flask, request, jsonify
from waitress import serve
import requests
import logging
import time
from datetime import datetime

app = Flask(__name__)

# ======================
# CONFIGURATION
# ======================
GREEN_API = {
    "idInstance": "7105258364",
    "apiToken": "9f9e1a1a2611446baed68fd648dba823d34e655958e54b28bb",
    "apiUrl": "https://7105.api.greenapi.com",
    "mediaUrl": "https://7105.media.greenapi.com"
}
AUTHORIZED_NUMBER = "923401809397"

# GLIF Configuration
GLIF_ID = "cm0zceq2a00023f114o6hti7w"
GLIF_TOKENS = [
    "glif_a4ef6d3aa5d8575ea8448b29e293919a42a6869143fcbfc32f2e4a7dbe53199a",
    "glif_51d216db54438b777c4170cd8913d628ff0af09789ed5dbcbd718fa6c6968bb1",
    "glif_c9dc66b31537b5a423446bbdead5dc2dbd73dc1f4a5c47a9b77328abcbc7b755",
    "glif_f5a55ee6d767b79f2f3af01c276ec53d14475eace7cabf34b22f8e5968f3fef5",
    "glif_c3a7fd4779b59f59c08d17d4a7db46beefa3e9e49a9ebc4921ecaca35c556ab7",
    "glif_b31fdc2c9a7aaac0ec69d5f59bf05ccea0c5786990ef06b79a1d7db8e37ba317"
]

# ======================
# LOGGING SETUP
# ======================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ======================
# GREENAPI FUNCTIONS
# ======================
def send_message(phone, text):
    """Send WhatsApp message via GreenAPI"""
    url = f"{GREEN_API['apiUrl']}/waInstance{GREEN_API['idInstance']}/sendMessage/{GREEN_API['apiToken']}"
    payload = {
        "chatId": f"{phone}@c.us",
        "message": text
    }
    headers = {'Content-Type': 'application/json'}
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        logger.info(f"Message sent to {phone}")
        return True
    except Exception as e:
        logger.error(f"Failed to send message: {str(e)}")
        return False

def send_image_url(phone, image_url, caption):
    """Send image via URL using GreenAPI"""
    # First upload the file
    upload_url = f"{GREEN_API['apiUrl']}/waInstance{GREEN_API['idInstance']}/uploadFile/{GREEN_API['apiToken']}"
    try:
        upload_response = requests.post(upload_url, json={"url": image_url})
        upload_response.raise_for_status()
        file_id = upload_response.json().get("idFile")
        if not file_id:
            raise ValueError("No file ID received")
    except Exception as e:
        logger.error(f"Upload failed: {str(e)}")
        return False

    # Then send the file
    send_url = f"{GREEN_API['apiUrl']}/waInstance{GREEN_API['idInstance']}/sendFileByUpload/{GREEN_API['apiToken']}"
    payload = {
        "chatId": f"{phone}@c.us",
        "caption": caption,
        "fileId": file_id
    }
    
    try:
        response = requests.post(send_url, json=payload)
        response.raise_for_status()
        logger.info(f"Image sent to {phone}")
        return True
    except Exception as e:
        logger.error(f"Failed to send image: {str(e)}")
        return False

# ======================
# GLIF FUNCTIONS
# ======================
def generate_thumbnail(prompt):
    """Generate thumbnail using GLIF API"""
    prompt = prompt[:100]  # Limit prompt length
    for token in GLIF_TOKENS:
        try:
            response = requests.post(
                f"https://simple-api.glif.app/{GLIF_ID}",
                headers={"Authorization": f"Bearer {token}"},
                json={"prompt": prompt, "style": "youtube_trending"},
                timeout=30
            )
            data = response.json()
            
            # Check all possible response formats
            for key in ["output", "image_url", "url"]:
                if key in data and isinstance(data[key], str) and data[key].startswith('http'):
                    logger.info(f"Generated thumbnail using token {token[-6:]}")
                    return {'status': 'success', 'image_url': data[key]}
        except Exception as e:
            logger.warning(f"GLIF token {token[-6:]} failed: {str(e)}")
    return {'status': 'error'}

# ======================
# WEBHOOK HANDLER
# ======================
@app.route('/webhook', methods=['POST'])
def handle_webhook():
    try:
        data = request.json
        logger.info(f"Incoming webhook: {data}")

        # Verify webhook type
        if data.get('typeWebhook') != 'incomingMessageReceived':
            logger.warning(f"Ignoring non-message webhook: {data.get('typeWebhook')}")
            return jsonify({'status': 'ignored'}), 200

        # Extract sender information
        sender_data = data.get('senderData', {})
        sender_phone = sender_data.get('sender', '').split('@')[0]
        
        # Verify authorized number
        if sender_phone != AUTHORIZED_NUMBER:
            logger.warning(f"Unauthorized access from: {sender_phone}")
            return jsonify({'status': 'unauthorized'}), 403

        # Extract message text
        message_data = data.get('messageData', {})
        if message_data.get('typeMessage') != 'textMessage':
            logger.warning("Received non-text message")
            return jsonify({'status': 'ignored'}), 200

        message = message_data.get('textMessageData', {}).get('textMessage', '').strip().lower()
        if not message:
            logger.warning("Received empty message")
            return jsonify({'status': 'empty_message'}), 200

        logger.info(f"Processing message from {sender_phone}: {message}")

        # Command handling
        if message in ['hi', 'hello', 'hey']:
            send_message(sender_phone, "👋 Hi! Send me any video topic to generate a thumbnail!")
        elif message in ['help', 'info']:
            send_message(sender_phone, "ℹ️ Just send me a video topic (e.g. 'cooking tutorial') and I'll create a thumbnail!")
        elif len(message) > 3:
            send_message(sender_phone, "🔄 Generating your thumbnail... (20-30 seconds)")
            result = generate_thumbnail(message)
            if result['status'] == 'success':
                send_image_url(sender_phone, result['image_url'], f"🎨 Thumbnail for: {message}")
                send_message(sender_phone, f"🔗 Direct URL: {result['image_url']}")
            else:
                send_message(sender_phone, "❌ Failed to generate. Please try different keywords.")

        return jsonify({'status': 'processed'})

    except Exception as e:
        logger.error(f"Webhook error: {str(e)}", exc_info=True)
        return jsonify({'status': 'error'}), 500

# ======================
# HEALTH CHECK
# ======================
@app.route('/')
def health_check():
    return jsonify({
        "status": "active",
        "authorized_number": AUTHORIZED_NUMBER,
        "instance_id": GREEN_API['idInstance'],
        "timestamp": datetime.now().isoformat()
    })

# ======================
# START SERVER
# ======================
if __name__ == '__main__':
    logger.info(f"""
    ============================================
    Starting WhatsApp Thumbnail Bot
    Authorized Number: {AUTHORIZED_NUMBER}
    GreenAPI Instance: {GREEN_API['idInstance']}
    ============================================
    """)
    serve(app, host='0.0.0.0', port=8000)
