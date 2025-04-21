import asyncio
import time
import signal
import traceback
import random
import logging
from aiohttp import web
from telegram.ext import Application

# Configure logging first
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

BOT_TOKEN = "7846379611:AAGzu4KM-Aq699Q8aHNt29t0YbTnDKbkXbI"

class EternalBot:
    def __init__(self, bot_token):
        self.bot_token = bot_token
        self.shutdown_event = asyncio.Event()
        self.restart_count = 0
        self.max_restarts = 100
        self.restart_window = 3600
        self.last_restart_time = time.time()
        self.application = None
        self.health_server = None
        self.network_retries = 5
        self.base_delay = 1.0

    async def setup_health_server(self):
        async def health_handler(request):
            return web.Response(text="OK")
        
        app = web.Application()
        app.router.add_get("/", health_handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", 8000)
        await site.start()
        return runner

    async def initialize_bot(self):
        if self.application:
            try:
                await self.application.stop()
                await self.application.shutdown()
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Cleanup error: {str(e)}")
        
        logger.info("Initializing new bot instance...")
        self.application = (
            Application.builder()
            .token(self.bot_token)
            .pool_timeout(30)
            .get_updates_pool_size(4)
            .build()
        )
        
        await self.application.initialize()
        await self.application.start()
        
        if self.application.updater:
            await self.application.updater.start_polling(
                poll_interval=0.5,
                timeout=10
            )
        
        logger.info("Bot initialized successfully")
        return self.application

    async def health_check(self):
        while not self.shutdown_event.is_set():
            try:
                if self.application and self.application.bot:
                    me = await self.application.bot.get_me()
                    logger.info(f"Health check OK - Bot ID: {me.id}")
                else:
                    logger.warning("Health check failed - Bot not initialized")
            except Exception as e:
                logger.error(f"Health check error: {str(e)}")
                await asyncio.sleep(5)
            else:
                await asyncio.sleep(30)

    async def run_forever(self):
        self.health_server = await self.setup_health_server()
        health_task = asyncio.create_task(self.health_check())
        
        while not self.shutdown_event.is_set():
            current_time = time.time()
            if current_time - self.last_restart_time > self.restart_window:
                self.restart_count = 0
            
            if self.restart_count >= self.max_restarts:
                logger.error(f"Max restarts ({self.max_restarts}) reached. Waiting...")
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
                logger.error(f"Bot crashed (restart {self.restart_count}/{self.max_restarts}): {str(e)}")
                traceback.print_exc()
                
                if self.shutdown_event.is_set():
                    break
                
                delay = min(self.base_delay * (2 ** min(self.restart_count, 5)), 300)
                delay *= (0.8 + 0.4 * random.random())
                logger.info(f"Restarting in {delay:.1f} seconds...")
                await asyncio.sleep(delay)
        
        health_task.cancel()
        try:
            await health_task
        except asyncio.CancelledError:
            pass
        finally:
            await self.health_server.cleanup()

async def shutdown_handler(signal, bot):
    logger.info(f"Received signal {signal.name}, shutting down...")
    bot.shutdown_event.set()
    
    if bot.application:
        logger.info("Stopping application...")
        await bot.application.stop()
        await bot.application.shutdown()
        
        if bot.application.updater:
            logger.info("Stopping updater...")
            await bot.application.updater.stop()
        
        await asyncio.sleep(1)

async def main():
    bot = EternalBot(BOT_TOKEN)
    loop = asyncio.get_running_loop()
    
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            sig,
            lambda s=sig: asyncio.create_task(shutdown_handler(s, bot))
        )
    
    try:
        await bot.run_forever()
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        traceback.print_exc()
    finally:
        logger.info("Bot shutdown complete")

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        logger.info("Starting eternal bot service...")
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    finally:
        pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
        if pending:
            loop.run_until_complete(asyncio.wait(pending, timeout=5))
        loop.close()
        logger.info("Event loop closed")
