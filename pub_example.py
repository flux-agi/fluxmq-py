import asyncio
import logging
from fluxmq.connection import FluxMQ

async def main():
    logging.basicConfig(level=logging.INFO)

    conn = await FluxMQ.Connect()
    if not conn:
        return

    try:
        while True:
            if await conn.Push("example/pub", b"big bo") is not None:
                break
            await asyncio.sleep(0.25)
    except KeyboardInterrupt:
        pass
    finally:
        await conn.Close()

if __name__ == "__main__":
    asyncio.run(main())