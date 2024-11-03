import asyncio
from logging import getLogger
from adapter.mqtt import MQTT, Topic
from runtime_service import RuntimeService


async def main():
    service_id = "runtime"
    service = RuntimeService(logger=getLogger("main"), service_id=service_id)
    service.attach(transport=MQTT(), topic_factory=Topic())
    await service.run()


asyncio.run(main())
