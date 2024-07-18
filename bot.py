import logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(lineno)d - %(module)s - %(levelname)s - %(message)s'
)
logging.getLogger().setLevel(logging.INFO)
logging.getLogger("pyrogram").setLevel(logging.WARNING)

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pyrogram import Client, types, idle
from config import Config, get_mongo_client
from database import Database

class channelforward(Client, Config):
    def __init__(self):
        super().__init__(
            name="CHANNELFORWARD",
            bot_token=self.BOT_TOKEN,
            api_id=self.API_ID,
            api_hash=self.API_HASH,
            workers=20,
            plugins={'root': 'Plugins'}
        )

        self.mongo_client = asyncio.run(get_mongo_client())
        self.db = self.mongo_client["channel_forward_bot"]
        self.queued_messages = self.db["queued_messages"]

    async def start(self):
        await super().start()
        me = await self.get_me()
        print(f"New session started for {me.first_name} ({me.username})")

        # Start the forwarding task using the Pyrogram event loop
        self._task = asyncio.ensure_future(self.send_queued_messages_loop())

    async def send_queued_messages_loop(self):
        while True:
            await self.send_queued_messages()
            await asyncio.sleep(60)  # Check for messages every 60 seconds

    async def send_queued_messages(self):
        now = datetime.now(ZoneInfo("Asia/Kolkata"))
        messages_data = self.queued_messages.find(
            {"trigger_time": {"$lte": now}}
        ).sort("trigger_time", 1).limit(Config.MAX_MESSAGES_PER_BATCH)
        
        async for msg_data in messages_data:
            try:
                message = types.Message(**msg_data["message_data"])
                await message.copy(int(msg_data["to_channel_id"]))
                logger.info(f"Forwarded a queued message from {msg_data['from_channel_id']} to {msg_data['to_channel_id']}")
            except Exception as e:
                logger.exception(e)
            finally:
                await self.queued_messages.delete_one({"_id": msg_data["_id"]})

    async def stop(self):
        await super().stop()
        self._task.cancel()  # Stop the task when the bot stops
        await self.mongo_client.close()  # Close MongoDB connection
        print("Session stopped. Bye!!")


if __name__ == "__main__":
    client = channelforward()
    client.run()
