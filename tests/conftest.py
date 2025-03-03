"""
Pytest configuration file for fluxmq-py tests.
"""
import pytest
import asyncio
from typing import Generator, AsyncGenerator

@pytest.fixture
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
async def mock_message() -> AsyncGenerator:
    """Create a mock message for testing."""
    from fluxmq.message import Message
    message = Message(topic="test/topic", data={"key": "value"})
    yield message 