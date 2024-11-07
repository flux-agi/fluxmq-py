import asyncio
from logging import Logger
from typing import Dict, Any, Callable
from asyncio import Task

from fluxmq.service import Service
from asyncio import Queue


class Node:
    logger: Logger
    service: Service
    output_topics: Dict[str, str]
    input_topics: Dict[str, str]
    input_tasks: list[Task]
    node_id: str
    status_callback_on_stop: Callable[[], None]
    status_callback_on_start: Callable[[], None]

    def __init__(self,
                 logger: Logger,
                 service: Service,
                 node_id: str,
                 output_topics: Dict[str, str],
                 input_topics: Dict[str, str]):
        self.logger = logger
        self.service = service
        self.output_topics = output_topics
        self.input_topics = input_topics
        self.node_id = node_id

    async def start(self) -> None:
        try:
            await self.on_start()
        except Exception as err:
            await self.__on_error(err)
            return

        async def read_input_queue(topic: str, queue: Queue):
            while True:
                msg = await queue.get()
                try:
                    await self.on_input(topic, msg)
                except Exception as err:
                    await self.__on_error(err)

        for topic in self.input_topics:
            queue = await self.service.subscribe(topic)
            task = asyncio.create_task(read_input_queue(topic, queue))
            task.add_done_callback(lambda t: None)
            self.input_tasks.append(task)

    async def stop(self) -> None:
        try:
            await self.on_stop()
        except Exception as err:
            self.logger.error(err)
        finally:
            for task in self.input_tasks:
                task.cancel()
            self.input_tasks.clear()

            for topic in self.input_topics:
                await self.service.unsubscribe(topic)

    async def destroy(self) -> None:
        await self.stop()
        await self.on_destroy()

    async def on_start(self) -> None:
        pass

    async def on_stop(self) -> None:
        pass

    async def on_destroy(self) -> None:
        pass

    async def on_error(self, err: Exception) -> None:
        pass

    async def on_input(self, topic: str, msg: Any):
        pass

    async def __on_error(self, err: Exception) -> None:
        self.logger.error(err)
        await self.on_error(err)
        await self.stop()
