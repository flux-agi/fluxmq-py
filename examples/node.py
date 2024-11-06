from fluxmq.message import Message
from fluxmq.node import Node
from fluxmq.service import Service
import time
import json
import asyncio
from logging import getLogger


class WorkNode(Node):
    async def on_start(self) -> None:
        while True:
            data = self.read()
            await self.on_data(data)
            await asyncio.sleep(0.001)
        pass

    async def on_data(self, data: bytes) -> None:
        pass

    async def read(self) -> bytes:
        pass


class WorkService(Service):
    nodes: list[WorkNode] = []

    async def on_configuration(self, message: Message):
        config = json.loads(message.payload.encode())

        for node in self.nodes:
            await node.stop()

        self.nodes = []

        for node_config_data in config['nodes']:
            self.nodes.append(Node(logger=getLogger()))

        for node in self.nodes:
            await node.start()

    async def on_shutdown(self, signal_number, frame):
        for node in self.nodes:
            await node.stop()


def main():
    pass


main()
