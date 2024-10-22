import asyncio
import logging
from fluxmq.connection import FluxMQ

async def main():
    logging.basicConfig(level=logging.INFO)
    ctx = asyncio.Event()  # Use asyncio.Event as a simple context
    conn = await FluxMQ.Connect()
    if not conn:
        return

    ch = await conn.Subscribe(ctx, "example/pub")

    async def process_messages():
        while True:
            msg = await ch.get()
            if msg is None:
                break
            if not await conn.Respond(msg, b"resp"):
                break

    asyncio.create_task(process_messages())

    await asyncio.sleep(10)
    ctx.set()  # Signal the event to stop the subscription
    await conn.Close()

if __name__ == "__main__":
    asyncio.run(main())
