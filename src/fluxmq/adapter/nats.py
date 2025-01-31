import asyncio
import threading
import nats

from asyncio import Queue
from logging import Logger, getLogger
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from typing import Dict

from fluxmq.message import Message
from fluxmq.node import NodeFactory, Node
from fluxmq.node import NodeState
from fluxmq.service import Service
from fluxmq.status import Status
from fluxmq.topic import Topic
from fluxmq.transport import Transport, SyncTransport


class Nats(Transport):
    connection = None
    logger: Logger
    servers: list[str]
    subscriptions: Dict[str, Subscription]

    def __init__(self, servers: list[str], logger=None):
        self.servers = servers
        self.subscriptions = {}
        if logger is None:
            self.logger = getLogger()
        else:
            self.logger = logger

    async def connect(self):
        self.connection = await nats.connect(servers=self.servers)
        self.logger.debug(f"Connected to {self.servers}")

    async def publish(self, topic: str, payload: bytes):
        if not isinstance(payload, bytes):
            payload = payload.encode('utf-8')
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

class SyncNats(SyncTransport):
    connection = None
    logger: Logger
    servers: list[str]
    subscriptions: Dict[str, Subscription]

    def __init__(self, servers: list[str], logger=None):
        self.servers = servers
        self.subscriptions = {}
        self.servers = servers
        self.nc = NATS()
        self.loop = None
        self.thread = None
        self.connected = False

        if logger is None:
            self.logger = getLogger()
        else:
            self.logger = logger

    def connect(self):
        self.loop = asyncio.get_event_loop()
        self.thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self.thread.start()

        future = asyncio.run_coroutine_threadsafe(self.nc.connect(servers=self.servers), self.loop)
        future.result()
        self.connected = True
        self.logger.debug(f"Connected to {self.servers}")

    def _run_event_loop(self):
        self.loop.run_forever()

    def publish(self, topic: str, payload: bytes):
        if not self.connected:
            raise RuntimeError("Not connected to NATS")
        future = asyncio.run_coroutine_threadsafe(self.nc.publish(topic, payload), self.loop)
        future.result()  # Wait for the publish to complete
        self.logger.debug("Sent message", extra={"topic": topic, "payload": payload})

    def subscribe(self, topic: str) -> Queue:
        print("subscribe to: ", topic)
        if not self.connected:
            raise RuntimeError("Not connected to NATS")

        queue = Queue()

        async def message_handler(msg):
            queue.put(msg)

        future = asyncio.run_coroutine_threadsafe(
            self.nc.subscribe(topic, cb=message_handler), self.loop
        )
        future.result()  # Wait for the subscription to complete
        return queue

    def unsubscribe(self, topic: str):
        if not self.connected:
            raise RuntimeError("Not connected to NATS")
        future = asyncio.run_coroutine_threadsafe(self.nc.unsubscribe(topic), self.loop)
        future.result()  # Wait for the unsubscribe to complete

    def close(self):
        if not self.connected:
            raise RuntimeError("Not connected to NATS")
        future = asyncio.run_coroutine_threadsafe(self.nc.close(), self.loop)
        future.result()  # Wait for the close to complete
        self.connected = False

    def request(self, topic: str, payload: bytes):
        pass  # Implement synchronous request logic if needed

    def respond(self, message: Message, response: bytes):
        if not self.connected:
            raise RuntimeError("Not connected to NATS")
        
        if message.reply is not None:
            future = asyncio.run_coroutine_threadsafe(self.nc.publish(message.reply, response), self.loop)
            future.result()  # Wait for the publish to complete

class NatsTopic(Topic):
    def set_node_state(self, node_id: str):
        return f"service/{node_id}/set_common_state"

    def get_node_state(self, node_id, str):
        return f"service/{node_id}/get_common_state"

    def start(self, service_id: str):
        return f"service/{service_id}/start"

    def stop(self, service_id: str):
        return f"service/{service_id}/stop"

    def node_state_request(self, service_id: str):
        return f"service/{service_id}/node_state_request"

    def request_configuration(self, service_id: str):
        return f"service/{service_id}/get_config"

    def restart_node(self, service_id: str):
        return f"service/{service_id}/restart"

    def time(self):
        return "service/tick"

    def control(self, service_id: str):
        return f"service/{service_id}/control"

    def status(self, service_id: str):
        return f"service/{service_id}/status"

    def configuration(self, service_id: str):
        return f"service/{service_id}/set_config"
    
    def settings(self, service_id: str):
        return f"service/{service_id}/set/settings"

    def configuration_request(self, service_id: str):
        return f"service/{service_id}/config_request"

    def status_request(self, service_id: str):
        return f"service/{service_id}/request_status"

    def error(self, service_id: str):
        return f"service/{service_id}/error"


class NatsStatus(Status):
    def connected(self):
        return "CONNECTED"

    def ready(self):
        return "READY"

    def active(self):
        return "ACTIVE"

    def paused(self):
        return "PAUSED"

    def error(self):
        return "ERROR"


class NatsNodeState(NodeState):

    def stopped(self):
        return "stopped"

    def started(self):
        return "started"


class NatsNodeFactory(NodeFactory):
    def create_node(self, service: Service) -> Node:
        return Node(logger=service.logger,
                    service=service,
                    state_factory=NatsNodeState())
