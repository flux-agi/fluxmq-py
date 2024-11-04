from asyncio import Queue

from fluxmq.message import Message
from fluxmq.transport import Transport
from fluxmq.topicfactory import TopicFactory


class MQTT(Transport):
    async def close(self) -> None:
        pass

    async def connect(self) -> None:
        pass

    async def publish(self, topic: str, payload):
        pass

    async def subscribe(self, topic: str) -> Queue[Message]:
        pass

    async def unsubscribe(self, topic: str):
        pass

    async def request(self, topic: str, payload):
        pass

    async def respond(self, topic: str, response):
        pass


class Topic(TopicFactory):
    def time(self):
        return f"time"

    def control(self, service_id: str):
        return f"service/{service_id}/control"

    def status(self, service_id: str):
        return f"service/{service_id}/status"

    def configuration(self, service_id: str):
        return f"service/{service_id}/configuration"
