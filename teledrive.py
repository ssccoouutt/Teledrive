import asyncio
import time
import signal
import traceback
from telegram.ext import Application

class EternalBot:
    def __init__(self, bot_token):
        self.bot_token = bot_token
        self.shutdown_event = asyncio.Event()
        self.restart_count = 0
        self.max_restarts = 100  # Maximum restarts per hour
        self.restart_window = 3600  # 1 hour window
        self.last_restart_time = time.time()
        self.application = None

    async def initialize_bot(self):
        """Create a fresh bot instance with proper cleanup"""
        if self.application:
            try:
                await self.application.stop()
                await self.application.shutdown()
                await asyncio.sleep(2)  # Cleanup grace period
            except Exception as e:
                print(f"⚠️ Cleanup error: {str(e)}")

        print("🔄 Initializing new bot instance...")
        self.application = Application.builder().token(self.bot_token).build()
        
        # Add minimal handlers (you can add your actual handlers here)
        self.application.add_error_handler(self.error_handler)
        
        await self.application.initialize()
        await self.application.start()
        if self.application.updater:
            await self.application.updater.start_polling()
        
        print("✅ Bot initialized successfully")
        return self.application

    async def error_handler(self, update, context):
        """Global error handler"""
        print(f"⚠️ Error occurred: {context.error}")
        traceback.print_exc()

    async def health_check(self):
        """Periodic health monitoring"""
        while not self.shutdown_event.is_set():
            try:
                # Simple check if bot is responsive
                if self.application and self.application.bot:
                    me = await self.application.bot.get_me()
                    print(f"❤️ Health check OK - Bot ID: {me.id}")
                else:
                    print("⚠️ Health check failed - Bot not initialized")
            except Exception as e:
                print(f"⚠️ Health check error: {str(e)}")
            
            await asyncio.sleep(30)  # Check every 30 seconds

    async def run_forever(self):
        """Main eternal run loop"""
        health_task = asyncio.create_task(self.health_check())
        
        while not self.shutdown_event.is_set():
            current_time = time.time()
            
            # Reset restart count if window has passed
            if current_time - self.last_restart_time > self.restart_window:
                self.restart_count = 0
            
            # Don't exceed maximum restarts
            if self.restart_count >= self.max_restarts:
                print(f"🛑 Max restarts ({self.max_restarts}) reached in last hour. Waiting...")
                await asyncio.sleep(self.restart_window)
                self.restart_count = 0
                continue
            
            try:
                await self.initialize_bot()
                self.last_restart_time = current_time
                
                # Keep the bot running until failure or shutdown
                while not self.shutdown_event.is_set():
                    await asyncio.sleep(1)
                    
            except Exception as e:
                self.restart_count += 1
                print(f"⚠️ Bot crashed (restart {self.restart_count}/{self.max_restarts}): {str(e)}")
                traceback.print_exc()
                
                if self.shutdown_event.is_set():
                    break
                    
                # Exponential backoff with jitter
                delay = min(5 * (2 ** min(self.restart_count, 5)), 300)  # Max 5 minutes
                delay *= (0.8 + 0.4 * random.random())  # Add jitter
                print(f"⏳ Restarting in {delay:.1f} seconds...")
                await asyncio.sleep(delay)
        
        health_task.cancel()
        try:
            await health_task
        except asyncio.CancelledError:
            pass

async def shutdown_handler(signal, bot):
    """Graceful shutdown handler"""
    print(f"\n🛑 Received signal {signal.name}, shutting down...")
    bot.shutdown_event.set()

async def main():
    bot = EternalBot(BOT_TOKEN)
    
    # Set up signal handlers
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            sig, 
            lambda s=sig: asyncio.create_task(shutdown_handler(s, bot))
    
    try:
        await bot.run_forever()
    except Exception as e:
        print(f"⚠️ Fatal error: {str(e)}")
        traceback.print_exc()
    finally:
        print("✅ Bot shutdown complete")

if __name__ == "__main__":
    # Configure event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        print("🚀 Starting eternal bot service...")
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user")
    finally:
        # Cleanup any remaining tasks
        pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
        if pending:
            loop.run_until_complete(asyncio.wait(pending, timeout=5))
        
        loop.close()
        print("✅ Event loop closed")

# Minimal configuration
BOT_TOKEN = "7846379611:AAGzu4KM-Aq699Q8aHNt29t0YbTnDKbkXbI"
