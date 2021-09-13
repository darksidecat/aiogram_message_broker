import asyncio
import logging

import aio_pika
from aiogram import Bot, Dispatcher
from aiogram.types import Message

import rate_limit_middleware
from sender import sender

logger = logging.getLogger(__name__)


BOT_TOKEN = "BOT TOKEN"


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )

    bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
    dp = Dispatcher()

    connection: aio_pika.Connection = await aio_pika.connect_robust(
        "amqp://guest:guest@127.0.0.1/",
    )
    channel: aio_pika.Channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)
    queue: aio_pika.Queue = await channel.declare_queue(
        rate_limit_middleware.QUEUE_NAME,
        durable=True,
        passive=True,
    )

    rate_limit_middleware.register(bot.session, connection, channel, queue)

    async def send_message(message: Message):
        return await message.answer(message.text)

    @dp.message()
    async def echo(message: Message):

        tasks = []
        for _ in range(10):
            tasks.append(send_message(message))

        result = await asyncio.gather(*tasks)

        for m in result:
            print(m)

    sender_task = asyncio.create_task(sender(connection, channel, queue, bot, 1))
    try:
        await dp.start_polling(bot)
    finally:
        await sender_task.cancel()
        await dp.fsm.storage.close()
        await bot.session.close()
        await connection.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.error("Bot stopped!")
