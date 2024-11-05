from abc import ABC, abstractmethod
from asyncio.queues import Queue

from message import Message


class Transport(ABC):
    @abstractmethod
    async def connect(self) -> None:
        pass

    @abstractmethod
    async def close(self) -> None:
        pass

    @abstractmethod
    async def publish(self, topic: str, payload: bytes):
        pass

    @abstractmethod
    async def subscribe(self, topic: str) -> Queue[Message]:
        pass

    @abstractmethod
    async def unsubscribe(self, topic: str):
        pass

    @abstractmethod
    async def request(self, topic: str, payload: bytes):
        pass

    @abstractmethod
    async def respond(self, message: Message, response: bytes):
        pass
