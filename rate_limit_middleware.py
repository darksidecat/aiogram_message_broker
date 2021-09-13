import asyncio
import uuid

import aio_pika


from typing import TypeVar, Callable, Awaitable, Optional

from aio_pika import DeliveryMode, IncomingMessage
from aiogram import Bot
from aiogram.methods import TelegramMethod, Response, SendMessage
from aiogram.types import UNSET, Message


T = TypeVar('T')
QUEUE_NAME = "send_queue"


class RequestLimitMiddleware:

    def __init__(self, connection, channel, queue):
        self.connection = connection
        self.channel = channel
        self.queue: aio_pika.Queue = queue
        self.callback_queue: Optional[aio_pika.Queue] = None
        self.futures = {}

    async def __call__(
        self,
        bot: Bot,
        method: TelegramMethod[T],
        make_request: Callable[[Bot, TelegramMethod[T]], Awaitable[Response[T]]],
    ):

        if not self.callback_queue:
            self.callback_queue = await self.channel.declare_queue(exclusive=True)

        if isinstance(method, SendMessage):
            if method.parse_mode == UNSET:
                if bot.parse_mode:
                    method.parse_mode = bot.parse_mode
                else:
                    method.parse_mode = None
            response = await self.publish_message(method)

            if response.type == "Message":
                return Message.parse_raw(response.body)
            else:
                return response.body  # Todo

        else:
            response = await make_request(bot, method)
            return response

    async def publish_message(self, method: TelegramMethod[T]):
        correlation_id = str(uuid.uuid4())

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.futures[correlation_id] = future

        await self.channel.default_exchange.publish(
            aio_pika.Message(
                body=method.json().encode(),
                content_type='application/json',
                correlation_id=correlation_id,
                delivery_mode=DeliveryMode.PERSISTENT,
                reply_to=self.callback_queue.name
            ),
            routing_key=QUEUE_NAME,
        )

        await self.callback_queue.consume(self.on_response)

        return await future

    def on_response(self, message: IncomingMessage):
        future = self.futures.pop(message.correlation_id)
        future.set_result(message)


def register(session, connection: aio_pika.Connection, channel, queue):
    session.middleware(RequestLimitMiddleware(connection, channel, queue))
