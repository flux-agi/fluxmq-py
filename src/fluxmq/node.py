import asyncio
from logging import Logger
from typing import Dict, Any
from asyncio import Task

from fluxmq.service import Service
from asyncio import Queue


class Node:
    logger: Logger
    service: Service
    output_topics: Dict[str, str]
    input_topics: Dict[str, str]
    input_tasks: list[Task]
    alias: str

    def __init__(self,
                 logger: Logger,
                 service: Service,
                 alias: str,
                 output_topics: Dict[str, str],
                 input_topics: Dict[str, str]):
        self.logger = logger
        self.service = service
        self.output_topics = output_topics
        self.input_topics = input_topics
        self.alias = alias

    async def start(self) -> None:
        await self.on_start()

        async def read_input_queue(topic: str, queue: Queue):
            while True:
                msg = await queue.get()
                await self.on_input(topic, msg)

        for topic in self.input_topics:
            queue = await self.service.subscribe(topic)
            task = asyncio.create_task(read_input_queue(topic, queue))
            task.add_done_callback(lambda t: None)
            self.input_tasks.append(task)

    async def stop(self) -> None:
        await self.on_stop()

        for task in self.input_tasks:
            task.cancel()
        self.input_tasks.clear()

    async def destroy(self) -> None:
        await self.stop()
        await self.on_destroy()

    async def on_start(self) -> None:
        pass

    async def on_stop(self) -> None:
        pass

    async def on_destroy(self) -> None:
        pass

    async def on_input(self, topic: str, msg: Any):
        pass
