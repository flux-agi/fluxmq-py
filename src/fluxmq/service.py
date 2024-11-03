import asyncio
import signal
import sys

from logging import Logger, getLogger
from asyncio.queues import Queue

from transport import Transport
from message import Message
from topicfactory import TopicFactory
from status import Status


class Service:
    transport: Transport
    topic_factory: TopicFactory
    id: str

    def __init__(self,
                 service_id=str,
                 logger: Logger = None):
        self.id = service_id
        if logger is None:
            self.logger = getLogger()

    def attach(self,
               transport: Transport,
               topic_factory: TopicFactory):
        self.transport = transport
        self.topic_factory = topic_factory
        return

    async def run(self,
                  shutdown_on_sigterm=True):

        await self.transport.connect()
        await self.__subscribe_configuration()
        await self.__subscribe_control()
        await self.send_status(Status.READY)

        if shutdown_on_sigterm:
            signal.signal(signal.SIGTERM, self.__graceful_shutdown)

        return

    async def __subscribe_configuration(self):
        topic = self.topic_factory.configuration(self.id)
        queue: Queue = await self.subscribe(topic)

        async def read_queue(queue: asyncio.queues.Queue[Message]):
            while True:
                message = await queue.get()
                self.on_configuration(message.payload)

        task = asyncio.create_task(read_queue(queue))
        task.add_done_callback(lambda t: None)

    async def __subscribe_control(self):
        topic = self.topic_factory.control(self.id)
        queue: Queue = await self.subscribe(topic)

        async def read_queue(queue: asyncio.queues.Queue[Message]):
            while True:
                message = await queue.get()
                self.on_control(message.payload)

        task = asyncio.create_task(read_queue(queue))
        task.add_done_callback(lambda t: None)

    async def __subscribe_time(self):
        topic = self.topic_factory.time()
        queue: Queue = await self.subscribe(topic)

        async def read_queue(queue: asyncio.queues.Queue[Message]):
            while True:
                message = await queue.get()
                time = int.from_bytes(message.payload, byteorder='big')
                self.on_time(time)

        task = asyncio.create_task(read_queue(queue))
        task.add_done_callback(lambda t: None)

    def __graceful_shutdown(self, signal_number, frame):
        self.on_shutdown(signal_number, frame)
        self.logger.debug("Shutting down gracefully %s, %s...", signal_number, frame)
        sys.exit(0)

    async def subscribe(self, topic: str) -> Queue:
        queue = await self.transport.subscribe(topic)
        return queue

    async def unsubscribe(self, topic: str):
        await self.transport.unsubscribe(topic)
        return

    async def publish(self, topic: str, message):
        await self.publish(topic, message)
        return

    async def request(self, topic: str, payload):
        await self.transport.request(topic, payload)
        return

    async def respond(self, message: Message, response: bytes):
        await self.transport.respond(message, response)
        return

    async def send_status(self, status: str):
        topic = self.topic_factory.status(self.id)
        await self.transport.publish(topic, status)

    def on_configuration(self, message: Message):
        pass

    def on_control(self, message: Message):
        pass

    def on_time(self, time: int):
        pass

    def on_shutdown(self, signal_number, frame):
        pass
