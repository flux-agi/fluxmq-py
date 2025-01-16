from asyncio import Task
from logging import Logger, getLogger
from typing import Coroutine, Any, Callable, Dict, TYPE_CHECKING

import asyncio
import sys

from asyncio.queues import Queue
from signal import signal, SIGTERM

from fluxmq.message import Message
from fluxmq.status import Status
from fluxmq.topic import Topic
from fluxmq.transport import Transport

if TYPE_CHECKING:
    from fluxmq.node import Node

class Service:
    transport: Transport
    topic: Topic
    status: Status
    id: str
    nodes: list['Node'] = []

    def __init__(self,
                 service_id: str = "unnamed",
                 logger: Logger = None):
        self.id = service_id
        if logger is None:
            self.logger = getLogger()

    def attach(self,
               transport: Transport,
               topic: Topic,
               status: Status) -> None:
        """
        attaches low level implementation of main abstractions
        :param transport:
        :param topic:
        :param status:
        :return:
        """

        self.transport = transport
        self.topic = topic
        self.status = status
        return

    async def run(self) -> None:
        await self.transport.connect()

        await self.subscribe_handler(self.topic.configuration(self.id), self.on_configuration)
        await self.subscribe_handler(self.topic.control(self.id), self.on_control)
        await self.subscribe_handler(self.topic.start(self.id), self.on_start)
        await self.subscribe_handler(self.topic.stop(self.id), self.on_stop)
        await self.subscribe_handler(self.topic.error(self.id), self.on_error)
        await self.subscribe_handler(self.topic.status(self.id), self.on_ready)
        await self.subscribe_handler(self.topic.restart_node(self.id), self.on_restart)

        signal(SIGTERM, self.__graceful_shutdown)

        await self.send_status(self.status.connected())
        await self.on_connected(self.id)

        return

    async def destroy_node_all(self) -> None:
        await self.destroy_node('*')

    async def start_node_all(self) -> None:
        await self.start_node('*')

    async def stop_node_all(self) -> None:
        await self.stop_node('*')

    async def destroy_node(self, node_id: str) -> None:
        for node in self.nodes:
            if node.node_id == node_id or node_id == '*':
                await node.destroy()
                self.nodes.remove(node)

    async def start_node(self, node_id: str) -> None:
        for node in self.nodes:
            if node.node_id == node_id or node_id == '*':
                await node.start()

    async def stop_node(self, node_id: str) -> None:
        for node in self.nodes:
            if node.node_id == node_id or node_id == '*':
                await node.stop()

    def append_node(self, node: 'Node') -> None:
        self.nodes.append(node)

    async def subscribe(self, topic: str) -> Queue:
        queue = await self.transport.subscribe(topic)
        return queue

    async def subscribe_handler(self, topic, handler: Callable[[Message], Coroutine[Any, Any, None]]) -> Task:
        queue: Queue = await self.subscribe(topic)

        async def read_queue(queue: asyncio.queues.Queue[Message]):
            while True:
                message = await queue.get()
                await handler(message)

        task = asyncio.create_task(read_queue(queue))
        task.add_done_callback(lambda t: None)
        return task

    async def unsubscribe(self, topic: str):
        await self.transport.unsubscribe(topic)
        return

    async def publish(self, topic: str, message):
        await self.transport.publish(topic, message)
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
        topic = self.topic.set_node_state(node_id)
        await self.transport.publish(topic, status)

    def __graceful_shutdown(self, signal_number, frame) -> None:
        self.logger.debug("Shutting down gracefully %s, %s...", signal_number, frame)

        async def callback():
            await self.destroy_node('*')
            await self.send_status(self.status.paused())
            await self.on_shutdown(signal_number, frame)
            await self.transport.close()

        asyncio.run(callback())
        sys.exit(0)

    async def on_connected(self, message: Message) -> None:
        pass

    async def on_ready(self, message: Message) -> None:
        pass

    async def on_start(self, message: Message) -> None:
        pass

    async def on_stop(self, message: Message) -> None:
        pass

    async def on_restart(self, message: Message) -> None:
        pass

    async def on_configuration(self, message: Message) -> None:
        pass

    async def on_control(self, message: Message) -> None:
        pass

    async def on_tick(self, time: int) -> None:
        pass

    async def on_shutdown(self, signal_number, frame) -> None:
        pass

    async def on_error (self, message: Message) -> None:
        pass
