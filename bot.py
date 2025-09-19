from telethon import TelegramClient, Button, events
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError
from aiohttp import web, ClientSession
from dotenv import load_dotenv
import os
import asyncio
import json
from datetime import datetime
import logging
import signal
import sys
import traceback
from typing import Dict, Any
import aiofiles

# Enhanced logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

# Configuration
api_id = int(os.getenv("API_ID"))
api_hash = os.getenv("API_HASH")
token = os.getenv("TOKEN")
bot_name = os.getenv("BOT_NAME", "Delayed Message Bot")
webhook_url = os.getenv("WEBHOOK_URL")  # e.g., https://yourdomain.com/webhook
webhook_port = int(os.getenv("WEBHOOK_PORT", 8080))
webhook_path = os.getenv("WEBHOOK_PATH", "/webhook")

# Global variables
bot_client = None
user_data = {}
user_sessions = {}
active_tasks = {}
webhook_app = web.Application()
shutdown_event = asyncio.Event()

DATA_FILE = "user_data.json"
MAX_RETRIES = 3
RETRY_DELAY = 5

class BotManager:
    """Enhanced bot manager with better error handling and recovery"""
    
    def __init__(self):
        self.is_running = False
        self.retry_count = 0
        
    async def start_bot_with_retry(self):
        """Start bot with retry mechanism"""
        while self.retry_count < MAX_RETRIES and not shutdown_event.is_set():
            try:
                await self.start_bot()
                self.retry_count = 0  # Reset on successful start
                break
            except Exception as e:
                self.retry_count += 1
                logger.error(f"Bot start attempt {self.retry_count} failed: {e}")
                if self.retry_count < MAX_RETRIES:
                    logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                    await asyncio.sleep(RETRY_DELAY)
                else:
                    logger.critical("Max retries reached. Bot startup failed.")
                    raise
    
    async def start_bot(self):
        """Start the bot with enhanced error handling"""
        global bot_client
        
        try:
            bot_client = TelegramClient("bot_session", api_id=api_id, api_hash=api_hash)
            await bot_client.start(bot_token=token)
            
            # Set webhook if URL is provided
            if webhook_url:
                try:
                    await bot_client.set_webhook(url=f"{webhook_url}{webhook_path}")
                    logger.info(f"Webhook set to: {webhook_url}{webhook_path}")
                except Exception as webhook_error:
                    logger.warning(f"Failed to set webhook: {webhook_error}")
                    logger.info("Falling back to polling mode...")
            
            self.is_running = True
            logger.info(f"Bot started successfully as {bot_name}")
            
        except Exception as e:
            logger.error(f"Failed to start bot: {e}")
            raise

async def load_user_data():
    """Async function to load user data"""
    global user_data
    try:
        if os.path.exists(DATA_FILE):
            async with aiofiles.open(DATA_FILE, 'r') as f:
                content = await f.read()
                user_data = json.loads(content)
                logger.info(f"Loaded data for {len(user_data)} users")
    except Exception as e:
        logger.error(f"Error loading user data: {e}")
        user_data = {}

async def save_user_data():
    """Async function to save user data"""
    try:
        safe_data = {}
        for user_id, data in user_data.items():
            safe_data[user_id] = {k: v for k, v in data.items() if k not in ['phone_code_hash', 'temp_client']}
        
        async with aiofiles.open(DATA_FILE, 'w') as f:
            await f.write(json.dumps(safe_data, indent=2))
    except Exception as e:
        logger.error(f"Error saving user data: {e}")

# Webhook handlers
async def webhook_handler(request):
    """Handle incoming webhook requests"""
    try:
        if request.method == 'GET':
            return web.Response(text="Webhook is active", status=200)
        
        if request.method != 'POST':
            return web.Response(text="Method not allowed", status=405)
        
        # Verify content type
        if not request.content_type.startswith('application/json'):
            return web.Response(text="Invalid content type", status=400)
        
        data = await request.json()
        
        # Process the update
        if bot_client and bot_client.is_connected():
            # Convert webhook data to Telethon update format if needed
            # This is a simplified example - you might need more complex handling
            await process_webhook_update(data)
        
        return web.Response(text="OK", status=200)
        
    except json.JSONDecodeError:
        logger.error("Invalid JSON in webhook request")
        return web.Response(text="Invalid JSON", status=400)
    except Exception as e:
        logger.error(f"Webhook handler error: {e}")
        return web.Response(text="Internal server error", status=500)

async def process_webhook_update(data: Dict[Any, Any]):
    """Process webhook updates"""
    try:
        # Add your webhook processing logic here
        # This depends on your specific webhook format
        logger.info(f"Processing webhook update: {data}")
        
        # Example: if you're receiving Telegram updates via webhook
        if 'message' in data:
            # Process message update
            pass
        elif 'callback_query' in data:
            # Process callback query update
            pass
            
    except Exception as e:
        logger.error(f"Error processing webhook update: {e}")

async def health_check_handler(request):
    """Health check endpoint"""
    status = {
        "status": "healthy" if bot_client and bot_client.is_connected() else "unhealthy",
        "timestamp": datetime.now().isoformat(),
        "active_tasks": len(active_tasks),
        "connected_users": len([s for s in user_sessions.values() if s.get('connected')])
    }
    return web.json_response(status)

# Enhanced error handling for message loops
async def delayed_message_loop(task_id):
    """Enhanced message loop with better error handling"""
    task_info = active_tasks.get(task_id)
    if not task_info:
        return
        
    user_id = task_info['user_id']
    consecutive_errors = 0
    max_consecutive_errors = 5
    
    try:
        user_client = user_sessions[user_id]['client']
        
        while task_id in active_tasks and active_tasks[task_id]['status'] in ['running', 'paused']:
            try:
                if active_tasks[task_id]['status'] == 'running':
                    await user_client.send_message(
                        task_info['chat_id'],
                        task_info['message']
                    )
                    active_tasks[task_id]['count'] += 1
                    consecutive_errors = 0  # Reset error counter on success
                    logger.info(f"Message sent for task {task_id}. Count: {active_tasks[task_id]['count']}")
                
                await asyncio.sleep(task_info['delay'])
                
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"Error sending message for task {task_id}: {e}")
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"Task {task_id} stopped due to too many consecutive errors")
                    break
                
                # Exponential backoff for errors
                error_delay = min(60, 2 ** consecutive_errors)
                await asyncio.sleep(error_delay)
    
    except Exception as e:
        logger.error(f"Critical error in delayed message loop {task_id}: {e}")
        logger.error(traceback.format_exc())
    
    finally:
        if task_id in active_tasks:
            del active_tasks[task_id]
            logger.info(f"Task {task_id} cleaned up")

# Button definitions (keeping original structure)
def inline_buttons():
    return [
        [
            Button.inline("🔗 Connect Account", data="connect"),
            Button.inline("❓ How to Use", data="help"),
        ]
    ]

def main_menu():
    return [
        [Button.inline("📤 Send Delayed Message", data="send_delayed")],
        [Button.inline("📋 My Active Messages", data="active_messages")],
        [Button.inline("⚙️ Settings", data="settings")],
        [Button.inline("🔌 Disconnect", data="disconnect")]
    ]

def structure_connect():
    return [
        [Button.inline("🆔 API ID", data="apiid")],
        [Button.inline("🔑 API HASH", data="apihash")],
        [Button.inline("📱 Phone Number", data="phone")],
        [Button.inline("🔗 Connect", data="dial")],
        [Button.inline("◀️ Back", data="back_start")]
    ]

def delayed_message_menu():
    return [
        [Button.inline("📝 Set Message", data="set_message")],
        [Button.inline("⏰ Set Delay (seconds)", data="set_delay")],
        [Button.inline("🎯 Select Chat/Group", data="select_chat")],
        [Button.inline("🚀 Start Sending", data="start_sending")],
        [Button.inline("◀️ Back", data="back_main")]
    ]

def message_control_menu(task_id):
    return [
        [
            Button.inline("⏸️ Pause", data=f"pause_{task_id}"),
            Button.inline("▶️ Resume", data=f"resume_{task_id}")
        ],
        [Button.inline("🛑 Stop", data=f"stop_{task_id}")],
        [Button.inline("◀️ Back", data="active_messages")]
    ]

# Event handlers with enhanced error handling
@bot_client.on(events.NewMessage(pattern="/start"))
async def start(event):
    try:
        user_id = str(event.sender_id)
        
        if user_id in user_sessions and user_sessions[user_id].get('connected'):
            await event.respond(
                f"<b>🎉 Welcome back! Your account is connected.</b>",
                buttons=main_menu(),
                parse_mode="HTML",
            )
        else:
            await event.respond(
                f"<b>👋 Hi! I'm {bot_name}!</b>\n\n"
                "I help you send delayed messages to your Telegram chats and groups.\n\n"
                "<b>Features:</b>\n"
                "• Send messages with custom delays\n"
                "• Control multiple message tasks\n"
                "• Pause/Resume/Stop functionality\n"
                "• Send to any chat or group you have access to",
                buttons=inline_buttons(),
                parse_mode="HTML",
            )
    except Exception as e:
        logger.error(f"Error in start handler: {e}")
        try:
            await event.respond("❌ An error occurred. Please try again.")
        except:
            pass

@bot_client.on(events.CallbackQuery(data=b"help"))
async def help_handler(event):
    try:
        help_text = """
<b>📖 How to Use:</b>

<b>1. Connect Your Account:</b>
• Get your API credentials from https://my.telegram.org
• Enter your API_ID, API_HASH, and phone number
• Verify with the code sent to your phone

<b>2. Send Delayed Messages:</b>
• Set your custom message
• Choose delay time (in seconds)
• Select target chat/group
• Start the automated sending

<b>3. Manage Active Messages:</b>
• View all running message tasks
• Pause/Resume any task
• Stop tasks when needed

<b>4. Settings:</b>
• View your connection status
• Manage connected chats
• Disconnect if needed
        """
        
        await event.edit(
            help_text,
            buttons=[[Button.inline("◀️ Back", data="back_start")]],
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Error in help handler: {e}")

# [Continue with all other event handlers - keeping the same logic but adding try/except blocks]
# I'll include a few key ones here and indicate the pattern:

@bot_client.on(events.CallbackQuery(data=b"connect"))
async def connect(event):
    try:
        user_id = str(event.sender_id)
        
        if user_id not in user_data:
            user_data[user_id] = {}
        
        current_data = user_data[user_id]
        status_text = "<b>🔧 Connection Setup:</b>\n\n"
        status_text += f"API ID: {'✅ Set' if current_data.get('api_id') else '❌ Not set'}\n"
        status_text += f"API Hash: {'✅ Set' if current_data.get('api_hash') else '❌ Not set'}\n"
        status_text += f"Phone: {'✅ Set' if current_data.get('phone') else '❌ Not set'}\n\n"
        status_text += "Set all values, then press Connect:"
        
        await event.edit(
            status_text,
            buttons=structure_connect(),
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error(f"Error in connect handler: {e}")

# Graceful shutdown handling
async def cleanup():
    """Cleanup function for graceful shutdown"""
    logger.info("Starting cleanup process...")
    
    # Stop all active tasks
    for task_id in list(active_tasks.keys()):
        del active_tasks[task_id]
    
    # Disconnect all user sessions
    for user_id, session in user_sessions.items():
        try:
            if session.get('client'):
                await session['client'].disconnect()
        except Exception as e:
            logger.error(f"Error disconnecting user {user_id}: {e}")
    
    # Save data
    await save_user_data()
    
    # Disconnect bot
    if bot_client and bot_client.is_connected():
        try:
            await bot_client.disconnect()
        except Exception as e:
            logger.error(f"Error disconnecting bot: {e}")
    
    logger.info("Cleanup completed")

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}. Shutting down gracefully...")
    shutdown_event.set()

async def setup_webhook_server():
    """Setup webhook server"""
    webhook_app.router.add_post(webhook_path, webhook_handler)
    webhook_app.router.add_get(webhook_path, webhook_handler)
    webhook_app.router.add_get('/health', health_check_handler)
    
    # Add CORS support
    webhook_app.router.add_route('OPTIONS', webhook_path, lambda req: web.Response())
    
    return webhook_app

async def run_webhook_server():
    """Run webhook server"""
    try:
        runner = web.AppRunner(webhook_app)
        await runner.setup()
        
        site = web.TCPSite(runner, '0.0.0.0', webhook_port)
        await site.start()
        
        logger.info(f"Webhook server started on port {webhook_port}")
        
        # Keep server running
        while not shutdown_event.is_set():
            await asyncio.sleep(1)
            
    except Exception as e:
        logger.error(f"Webhook server error: {e}")
    finally:
        await runner.cleanup()

async def main():
    """Main function with enhanced error handling"""
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Load user data
        await load_user_data()
        
        # Setup webhook server
        await setup_webhook_server()
        
        # Initialize bot manager
        bot_manager = BotManager()
        
        # Create tasks
        tasks = []
        
        # Start webhook server if configured
        if webhook_url:
            tasks.append(asyncio.create_task(run_webhook_server()))
        
        # Start bot
        tasks.append(asyncio.create_task(bot_manager.start_bot_with_retry()))
        
        # Run bot
        if not webhook_url:
            # If no webhook, run in polling mode
            tasks.append(asyncio.create_task(bot_client.run_until_disconnected()))
        else:
            # If webhook mode, just keep alive
            tasks.append(asyncio.create_task(keep_alive()))
        
        # Wait for shutdown signal
        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            logger.info("Received KeyboardInterrupt")
        
    except Exception as e:
        logger.critical(f"Critical error in main: {e}")
        logger.critical(traceback.format_exc())
    finally:
        await cleanup()

async def keep_alive():
    """Keep the bot alive in webhook mode"""
    while not shutdown_event.is_set():
        try:
            # Periodic health check
            if bot_client and not bot_client.is_connected():
                logger.warning("Bot disconnected. Attempting to reconnect...")
                await bot_client.connect()
            
            await asyncio.sleep(30)  # Check every 30 seconds
        except Exception as e:
            logger.error(f"Keep alive error: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot shutdown requested")
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        sys.exit(1)
