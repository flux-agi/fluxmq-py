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
    async def on_configuration(self, message: Message):
        config = json.loads(message.payload.encode())

        await self.stop_nodes()
        await self.clear_nodes()

        for node_config_data in config['nodes']:
            output_topics = node_config_data['output_topics']
            input_topics = node_config_data['input_topics']
            alias = node_config_data['alias']
            self.append_node(Node(logger=getLogger(),
                                  output_topics=output_topics,
                                  input_topics=input_topics,
                                  service=self,
                                  alias=alias))

        await self.start_nodes()


def main():
    pass


main()
