# FluxMQ Python Library

A flexible, protocol-agnostic messaging library for Python applications, providing a unified interface for different messaging protocols.

## Overview

FluxMQ is a messaging abstraction layer that allows applications to use different messaging protocols (NATS, MQTT, etc.) through a consistent interface. This makes it easy to switch between messaging systems without changing application code.

Key features:
- Protocol-agnostic messaging interface
- Support for multiple transport implementations (NATS, MQTT)
- Asynchronous and synchronous APIs
- Request-reply pattern support
- Standardized topic naming conventions
- Consistent status reporting

## Installation

```bash
pip install fluxmq
```

Or install from source:

```bash
git clone https://github.com/yourusername/fluxmq-py.git
cd fluxmq-py
pip install -e .
```

## Dependencies

- Python 3.7+
- For NATS support: `nats-py`
- For MQTT support: `paho-mqtt`
- For Zenoh support: `zenoh` (install with `pip install fluxmq[zenoh]`)

## Quick Start

### Basic Usage with NATS

```python
import asyncio
from fluxmq.adapter.nats import Nats, NatsTopic, NatsStatus
from fluxmq.message import Message

async def main():
    # Initialize transport, topic, and status
    transport = Nats(servers=["nats://localhost:4222"])
    topic = NatsTopic()
    status = NatsStatus()
    
    # Connect to the messaging system
    await transport.connect()
    
    # Subscribe to a topic
    async def message_handler(message: Message):
        print(f"Received message: {message.get_data_as_string()}")
        
    await transport.subscribe("example.topic", message_handler)
    
    # Publish a message
    await transport.publish("example.topic", "Hello, world!")
    
    # Request-reply pattern
    response = await transport.request("example.service", {"action": "get_data"})
    print(f"Response: {response.get_data_as_string()}")
    
    # Clean up
    await transport.close()

if __name__ == "__main__":
    asyncio.run(main())
```

### Using MQTT

```python
import asyncio
from fluxmq.adapter.mqtt import MQTT, MQTTTopic, MQTTStatus
from fluxmq.message import Message

async def main():
    # Initialize transport, topic, and status
    transport = MQTT(host="localhost", port=1883)
    topic = MQTTTopic()
    status = MQTTStatus()
    
    # Connect to the messaging system
    await transport.connect()
    
    # Subscribe to a topic
    async def message_handler(message: Message):
        print(f"Received message: {message.get_data_as_string()}")
        
    await transport.subscribe("example/topic", message_handler)
    
    # Publish a message
    await transport.publish("example/topic", "Hello, world!")
    
    # Clean up
    await transport.close()

if __name__ == "__main__":
    asyncio.run(main())
```

### Using Zenoh

```python
import asyncio
from fluxmq.adapter.zenoh import Zenoh, ZenohTopic, ZenohStatus
from fluxmq.message import Message

async def main():
    # Initialize transport, topic, and status
    transport = Zenoh()  # Default configuration connects to local Zenoh router
    topic = ZenohTopic()
    status = ZenohStatus()
    
    # Connect to the messaging system
    await transport.connect()
    
    # Subscribe to a topic
    async def message_handler(message: Message):
        print(f"Received message: {message.get_data_as_string()}")
        
    await transport.subscribe("example/topic", message_handler)
    
    # Publish a message
    await transport.publish("example/topic", "Hello, world!")
    
    # Request-reply pattern
    response = await transport.request("example/service", {"action": "get_data"})
    print(f"Response: {response.get_data_as_string()}")
    
    # Clean up
    await transport.close()

if __name__ == "__main__":
    asyncio.run(main())
```

## Architecture

FluxMQ consists of several key components:

### Transport

The `Transport` interface defines the core messaging operations:
- `connect()`: Connect to the messaging system
- `close()`: Close the connection
- `publish()`: Publish a message to a topic
- `subscribe()`: Subscribe to a topic with a message handler
- `unsubscribe()`: Unsubscribe from a topic
- `request()`: Send a request and wait for a response
- `respond()`: Respond to a request message

Implementations:
- `Nats`: NATS implementation
- `MQTT`: MQTT implementation
- `Zenoh`: Zenoh implementation

### Message

The `Message` class represents a message in the system:
- `data`: The message payload
- `reply`: Optional reply subject for request-reply patterns
- `headers`: Optional headers associated with the message

### Topic

The `Topic` interface provides standardized topic naming conventions:
- `status()`: Topic for service status updates
- `configuration()`: Topic for service configuration
- `error()`: Topic for error messages
- And many more...

Implementations:
- `StandardTopic`: Base implementation with configurable prefix
- `NatsTopic`: NATS-specific implementation
- `MQTTTopic`: MQTT-specific implementation
- `ZenohTopic`: Zenoh-specific implementation

### Status

The `Status` interface provides standardized status values:
- `connected()`: Connected status
- `ready()`: Ready status
- `active()`: Active status
- `paused()`: Paused status
- `error()`: Error status

Implementations:
- `StandardStatus`: Base implementation
- `NatsStatus`: NATS-specific implementation
- `MQTTStatus`: MQTT-specific implementation
- `ZenohStatus`: Zenoh-specific implementation

## Advanced Usage

### Synchronous API

For applications that can't use asyncio, FluxMQ provides a synchronous API:

```python
from fluxmq.adapter.nats import SyncNats
from fluxmq.message import Message

def message_handler(message: Message):
    print(f"Received message: {message.get_data_as_string()}")

# Initialize and connect
transport = SyncNats(servers=["nats://localhost:4222"])
transport.connect()

# Subscribe to a topic
transport.subscribe("example.topic", message_handler)

# Publish a message
transport.publish("example.topic", "Hello, world!")

# Clean up
transport.close()
```

### Custom Transport Implementation

You can implement your own transport by extending the `Transport` interface:

```python
from fluxmq.transport import Transport
from fluxmq.message import Message
from typing import Callable, Awaitable, Any, Union

class CustomTransport(Transport):
    async def connect(self) -> None:
        # Implementation...
        
    async def close(self) -> None:
        # Implementation...
        
    async def publish(self, topic: str, payload: Union[bytes, str]) -> None:
        # Implementation...
        
    async def subscribe(self, topic: str, handler: Callable[[Message], Awaitable[None]]) -> Any:
        # Implementation...
        
    async def unsubscribe(self, topic: str) -> None:
        # Implementation...
        
    async def request(self, topic: str, payload: Union[bytes, str]) -> Message:
        # Implementation...
        
    async def respond(self, message: Message, response: Union[bytes, str]) -> None:
        # Implementation...
```

## Testing

FluxMQ includes a comprehensive test suite to ensure reliability and correctness. To run the tests:

1. Install the development dependencies:

```bash
pip install -e ".[dev]"
# or
pip install -r requirements-dev.txt
```

2. Run the tests using pytest:

```bash
pytest
```

For more verbose output:

```bash
pytest -v
```

To run only specific tests:

```bash
# Run tests for a specific module
pytest tests/test_message.py

# Run tests for a specific class
pytest tests/test_transport.py::TestTransport

# Run a specific test
pytest tests/test_message.py::test_message_init
```

To run tests with coverage reporting:

```bash
coverage run -m pytest
coverage report
coverage html  # Generates an HTML report in htmlcov/
```

## License

[MIT License](LICENSE)

