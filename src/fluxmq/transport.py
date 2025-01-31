from abc import ABC, abstractmethod
from asyncio.queues import Queue
from typing import Callable

from fluxmq.message import Message


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
    async def subscribe(self, topic: str, handler: Callable[[Message], None]):
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

    
    from abc import ABC, abstractmethod
from queue import Queue

from fluxmq.message import Message


class SyncTransport(ABC):
    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def publish(self, topic: str, payload: bytes) -> None:
        pass

    @abstractmethod
    def subscribe(self, topic: str) -> Queue[Message]:
        pass

    @abstractmethod
    def unsubscribe(self, topic: str) -> None:
        pass

    @abstractmethod
    def request(self, topic: str, payload: bytes) -> None:
        pass

    @abstractmethod
    def respond(self, message: Message, response: bytes) -> None:
        pass