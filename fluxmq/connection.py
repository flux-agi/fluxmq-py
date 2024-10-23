import asyncio
import logging
import nats
from fluxmq.message import Message

DEFAULT_URL = "nats://localhost:4222"

class FluxMQ:
    def __init__(self, host=None):
        self.host = host if host else DEFAULT_URL
        self.logger = logging.getLogger("fluxmq")
        self.connection = None
        self.subscriptions = {}

    @classmethod
    async def Connect(cls, *opts):
        self = cls()
        for opt in opts:
            opt(self)
        self.set_defaults_if_needed()
        try:
            self.connection = await nats.connect(servers=[self.host])
            self.logger.info(f"Connected to {self.host}")
        except Exception as e:
            self.logger.error(f"Cannot connect to {self.host}: {e}")
            return None
        return self

    def set_defaults_if_needed(self):
        if not self.host:
            self.host = DEFAULT_URL
        if not self.logger:
            self.logger = logging.getLogger("fluxmq")

    async def Subscribe(self, ctx, topic):
        ch = asyncio.Queue()

        async def message_handler(message):
            self.logger.info(f"Received message: {message.data.decode()}")
            await ch.put(Message.newDomainMsg(message))

        try:
            subscription = await self.connection.subscribe(topic, cb=message_handler)
            self.logger.info(f"Subscribed to topic: {topic}")
            self.subscriptions[topic] = subscription
        except Exception as e:
            self.logger.error(f"Failed to subscribe to topic {topic}: {e}")
            return None

        async def Unsubscribe():
            await self.subscriptions[topic].unsubscribe()
            await ch.put(None)

        async def ctx_done_listener():
            await ctx.wait()
            await Unsubscribe()

        self.unsubscribe_task = asyncio.create_task(ctx_done_listener())
        return ch

    async def Push(self, topic, message):
        try:
            await self.connection.publish(topic, message)
            self.logger.info("Sent message", extra={"topic_name": topic, "msg_content": message.decode()})
        except Exception as e:
            self.logger.error(e)
            return e

    async def Respond(self, message, data):
        try:
            await message.Respond(data)
        except ValueError as e:
            self.logger.error(e)

    async def Close(self):
        if hasattr(self, 'unsubscribe_task'):
            await self.unsubscribe_task
        await self.connection.close()

    async def Call(self, topic, data):
        try:
            resp = await self.connection.request(topic, data, timeout=1)
            return Message.newDomainMsg(resp)
        except Exception as e:
            if 'No Responders' in str(e):
                self.logger.error(f"No responders for topic: {topic}")
            else:
                self.logger.error(e)
            return None
