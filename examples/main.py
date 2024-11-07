from logging import getLogger

import asyncio

from fluxmq.adapter.mqtt import MQTT, Topic, Status as Status
from runtime_service import RuntimeService


async def main():
    service_id = "runtime"
    service = RuntimeService(logger=getLogger("main"), service_id=service_id)
    service.attach(transport=MQTT(), status=ServiceStatus(), topic=Topic())
    await service.run()


asyncio.run(main())
