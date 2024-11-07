from logging import Logger, getLogger

import asyncio
import sys

from asyncio.queues import Queue
from signal import signal, SIGTERM

from fluxmq.message import Message
from fluxmq.service_status_factory import ServiceStatusFactory
from fluxmq.topicfactory import TopicFactory
from fluxmq.transport import Transport
from fluxmq.node import Node


class Service:
    transport: Transport
    topic: TopicFactory
    status: ServiceStatusFactory
    id: str
    nodes: list[Node]

    def __init__(self,
                 service_id=str,
                 logger: Logger = None):
        self.id = service_id
        if logger is None:
            self.logger = getLogger()

    def attach(self,
               transport: Transport,
               topic: TopicFactory, status: ServiceStatusFactory) -> None:
        self.transport = transport
        self.topic = topic
        self.status = status
        return

    async def run(self) -> None:

        await self.transport.connect()
        await self.__subscribe_configuration()
        await self.__subscribe_control()
        await self.send_status(self.status.up())

        signal(SIGTERM, self.__graceful_shutdown)

        return

    async def destroy_nodes(self) -> None:
        for node in self.nodes:
            await node.destroy()
        self.nodes.clear()

    async def start_nodes(self) -> None:
        for node in self.nodes:
            await node.start()

    async def stop_nodes(self) -> None:
        for node in self.nodes:
            await node.stop()

    def append_node(self, node: Node) -> None:
        self.nodes.append(node)

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
        topic = self.topic.status(self.id)
        await self.transport.publish(topic, status)

    async def send_node_state(self, node_id: str, status: str):
        topic = self.topic.node_state(node_id)
        await self.transport.publish(topic, status)

    async def on_configuration(self, message: Message):
        pass

    async def on_control(self, message: Message):
        pass

    async def on_time(self, time: int):
        pass

    async def on_shutdown(self, signal_number, frame):
        pass

    async def __subscribe_configuration(self) -> None:
        topic = self.topic.configuration(self.id)
        queue: Queue = await self.subscribe(topic)

        async def read_queue(queue: asyncio.queues.Queue[Message]):
            while True:
                message = await queue.get()
                await self.on_configuration(message)

        task = asyncio.create_task(read_queue(queue))
        task.add_done_callback(lambda t: None)
        return

    async def __subscribe_control(self) -> None:
        topic = self.topic.control(self.id)
        queue: Queue = await self.subscribe(topic)

        async def read_queue(queue: asyncio.queues.Queue[Message]):
            while True:
                message = await queue.get()
                await self.on_control(message)

        task = asyncio.create_task(read_queue(queue))
        task.add_done_callback(lambda t: None)
        return

    async def __subscribe_time(self) -> None:
        topic = self.topic.time()
        queue: Queue = await self.subscribe(topic)

        async def read_queue(queue: asyncio.queues.Queue[Message]):
            while True:
                message = await queue.get()
                time = int.from_bytes(message.payload, byteorder='big')
                await self.on_time(time)

        task = asyncio.create_task(read_queue(queue))
        task.add_done_callback(lambda t: None)
        return

    def __graceful_shutdown(self, signal_number, frame) -> None:
        self.logger.debug("Shutting down gracefully %s, %s...", signal_number, frame)
        self.stop_nodes()
        self.destroy_nodes()
        self.send_status(self.status.down())
        self.on_shutdown(signal_number, frame)
        self.transport.close()
        sys.exit(0)
