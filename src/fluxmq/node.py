from logging import Logger

from abc import ABC, abstractmethod

from fluxmq.service import Service


class Node(ABC):
    service: Service

    def __init__(self,
                 logger: Logger,
                 service: Service):
        self.service = service
        self.logger = logger

    def start(self) -> None:
        self.on_start()

    def stop(self) -> None:
        self.on_stop()

    def destroy(self) -> None:
        self.stop()
        self.on_destroy()

    async def on_start(self) -> None:
        pass

    async def on_stop(self) -> None:
        pass

    async def on_destroy(self) -> None:
        pass


class NodeStatus(ABC):
    pass


class NodeTopic(ABC):
    @abstractmethod
    def status(self):
        pass

    @abstractmethod
    def send_status(self):
        pass
