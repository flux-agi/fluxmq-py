from asyncio import Queue
from logging import Logger, getLogger

import asyncio
import nats
from fluxmq.message import Message
from fluxmq.status import Status
from fluxmq.topic import Topic
from fluxmq.transport import Transport
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from typing import Dict


class Nats(Transport):
    connection = None
    logger: Logger
    servers: list[str]
    subscriptions: Dict[str, Subscription]

    def __init__(self, servers: list[str], logger=None):
        self.servers = servers
        if logger is None:
            self.logger = getLogger()
        else:
            self.logger = logger

    async def connect(self):
        self.connection = await nats.connect(servers=self.servers)
        self.logger.debug(f"Connected to {self.servers}")

    async def publish(self, topic: str, payload: bytes):
        await self.connection.publish(topic, payload)
        self.logger.debug("Sent message", extra={"topic": topic, "payload": payload})

    async def subscribe(self, topic: str) -> Queue[Message]:
        queue = asyncio.Queue()

        async def message_handler(raw: Msg):
            message = Message(reply=raw.reply, payload=raw.data)
            await queue.put(message)

        subscription = await self.connection.subscribe(topic, cb=message_handler)
        self.subscriptions[topic] = subscription
        self.logger.debug(f"Subscribed to topic: {topic}")
        return queue

    async def unsubscribe(self, topic: str):
        subscription = self.subscriptions[topic]
        if subscription is not None:
            await subscription.unsubscribe()

    async def request(self, topic: str, payload: bytes):
        pass

    async def respond(self, message: Message, response: bytes):
        if message.reply is not None:
            await self.connection.publish(message.reply)

    async def close(self) -> None:
        await self.connection.close()


class NatsTopic(Topic):
    def node_state(self, node_id: str):
        pass

    def start(self, service_id: str):
        pass

    def stop(self, service_id: str):
        pass

    def node_state_request(self, service_id: str):
        pass

    def request_configuration(self, service_id: str):
        return f"service/get_config"

    def time(self):
        return "time"

    def control(self, service_id: str):
        return f"service.{service_id}.control"

    def status(self, service_id: str):
        return f"service.{service_id}.status"

    def configuration(self, service_id: str):
        return f"service.{service_id}.configuration"

    def configuration_request(self, service_id: str):
        pass

    def status_request(self, service_id: str):
        pass


class NatsStatus(Status):
    def up(self):
        return "up"

    def down(self):
        return "down"

    def started(self):
        return "ready"

    def stopped(self):
        return "paused"
