import asyncio
from typing import Union

import aio_pika
from aiogram import Bot
from aiogram.methods import SendMessage


async def sender(
        connection: aio_pika.Connection,
        channel: aio_pika.Channel,
        queue: aio_pika.Queue,
        bot: Bot,
        rate_limit: Union[int, float] = 1/30,
        grace_shutdown = True
):
    async with connection:
        # Declaring queue
        async for message in queue:
            async with message.process(requeue=True, ignore_processed=True):
                try:

                    msg: SendMessage = SendMessage.parse_raw(message.body)

                    response = await bot.session.make_request(bot=bot, call=msg)
                    message: aio_pika.IncomingMessage

                    asyncio.create_task(
                        channel.default_exchange.publish(
                            aio_pika.Message(
                                body=response.json().encode(),
                                type="Message",
                                correlation_id=message.correlation_id
                            ),
                            routing_key=message.reply_to
                        )
                    )
                    message.ack()
                    await asyncio.sleep(rate_limit)

                    if queue.name in message.body.decode():
                        break
                except asyncio.CancelledError as err:  # Can`t be caught from create_task?
                    await queue.declare()
                    message_count = queue.declaration_result.message_count
                    if grace_shutdown and message_count > 0:
                        print("Wait for empty queue")
                        while True:
                            await queue.declare()
                            message_count = queue.declaration_result.message_count
                            if message_count == 0:
                                break

                    else:
                        raise err
