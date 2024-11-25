import asyncio
import json

from typing import Any
from logging import getLogger

from fluxmq.adapter.nats import Nats, NatsTopic, NatsStatus
from fluxmq.service import Service
from fluxmq.message import Message
from fluxmq.node import Node


class Runtime:
    """
    some runtime implementation
    """

    def start(self):
        pass

    def stop(self):
        pass


class RuntimeNode(Node):
    runtime: Any

    async def on_create(self) -> None:
        self.runtime = Runtime()

    async def on_start(self) -> None:
        self.logger.debug(f"Node started.")
        """
        should start Runtime engine here
        """
        self.runtime.start()
        return

    async def on_stop(self) -> None:
        self.logger.debug(f"Node stopped.")
        """
        should stop Runtime engine here
        """
        self.runtime.stop()
        return


class RuntimeService(Service):
    async def on_configuration(self, message: Message) -> None:
        config = json.loads(message.payload.encode())

        node = RuntimeNode(service=self,
                           node_id=config['node_id'],
                           output_topics=config['output_topics'],
                           input_topics=config['input_topics'])
        self.append_node(node)

    async def on_start(self, message: Message) -> None:
        await self.start_node_all()

    async def on_stop(self, message: Message) -> None:
        await self.stop_node_all()

    async def on_control(self, message: Message):
        data = json.loads(message.payload.encode())
        if data['command'] == "set":
            self.logger.debug(f"Executing set command.")
        return

    async def on_shutdown(self, signal_number, frame):
        self.logger.debug(f"Shutting down service.")
        pass

    async def on_time(self, time: int):
        self.logger.debug(f"System coordinated time: {time}")
        pass


async def main():
    service = RuntimeService(logger=getLogger("main"),
                             service_id="runtime")
    service.attach(transport=Nats(['nats://127.0.0.1:4222']),
                   status=NatsStatus(),
                   topic=NatsTopic())
    await service.run()


asyncio.run(main())
