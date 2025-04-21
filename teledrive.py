import asyncio
import time
import signal
import traceback
import random
from telegram.ext import Application

class EternalBot:
    def __init__(self, bot_token):
        self.bot_token = bot_token
        self.shutdown_event = asyncio.Event()
        self.restart_count = 0
        self.max_restarts = 100
        self.restart_window = 3600
        self.last_restart_time = time.time()
        self.application = None

    async def initialize_bot(self):
        if self.application:
            try:
                await self.application.stop()
                await self.application.shutdown()
                await asyncio.sleep(2)
            except Exception as e:
                print(f"⚠️ Cleanup error: {str(e)}")
        print("🔄 Initializing new bot instance...")
        self.application = Application.builder().token(self.bot_token).build()
        await self.application.initialize()
        await self.application.start()
        if self.application.updater:
            await self.application.updater.start_polling()
        print("✅ Bot initialized successfully")
        return self.application

    async def health_check(self):
        while not self.shutdown_event.is_set():
            try:
                if self.application and self.application.bot:
                    me = await self.application.bot.get_me()
                    print(f"❤️ Health check OK - Bot ID: {me.id}")
                else:
                    print("⚠️ Health check failed - Bot not initialized")
            except Exception as e:
                print(f"⚠️ Health check error: {str(e)}")
            await asyncio.sleep(30)

    async def run_forever(self):
        health_task = asyncio.create_task(self.health_check())
        while not self.shutdown_event.is_set():
            current_time = time.time()
            if current_time - self.last_restart_time > self.restart_window:
                self.restart_count = 0
            if self.restart_count >= self.max_restarts:
                print(f"🛑 Max restarts ({self.max_restarts}) reached in last hour. Waiting...")
                await asyncio.sleep(self.restart_window)
                self.restart_count = 0
                continue
            try:
                await self.initialize_bot()
                self.last_restart_time = current_time
                while not self.shutdown_event.is_set():
                    await asyncio.sleep(1)
            except Exception as e:
                self.restart_count += 1
                print(f"⚠️ Bot crashed (restart {self.restart_count}/{self.max_restarts}): {str(e)}")
                traceback.print_exc()
                if self.shutdown_event.is_set():
                    break
                delay = min(5 * (2 ** min(self.restart_count, 5)), 300)
                delay *= (0.8 + 0.4 * random.random())
                print(f"⏳ Restarting in {delay:.1f} seconds...")
                await asyncio.sleep(delay)
        health_task.cancel()
        try:
            await health_task
        except asyncio.CancelledError:
            pass

async def shutdown_handler(signal, bot):
    print(f"\n🛑 Received signal {signal.name}, shutting down...")
    bot.shutdown_event.set()

async def main():
    bot = EternalBot(BOT_TOKEN)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            sig, 
            lambda s=sig: asyncio.create_task(shutdown_handler(s, bot))
        )  # Corrected line
    try:
        await bot.run_forever()
    except Exception as e:
        print(f"⚠️ Fatal error: {str(e)}")
        traceback.print_exc()
    finally:
        print("✅ Bot shutdown complete")

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        print("🚀 Starting eternal bot service...")
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user")
    finally:
        pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
        if pending:
            loop.run_until_complete(asyncio.wait(pending, timeout=5))
        loop.close()
        print("✅ Event loop closed")

BOT_TOKEN = "7846379611:AAGzu4KM-Aq699Q8aHNt29t0YbTnDKbkXbI"
