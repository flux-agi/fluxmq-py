"""
Tests for the Transport interface.
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any, Dict

from fluxmq.transport import Transport, SyncTransport, TypedQueue
from fluxmq.message import Message


class TestTypedQueue:
    """Tests for the TypedQueue class."""
    
    def test_typed_queue_init(self):
        """Test TypedQueue initialization."""
        queue = TypedQueue[str]()
        assert queue.maxsize == 0  # Default maxsize
        
        queue_with_maxsize = TypedQueue[Dict[str, Any]](maxsize=10)
        assert queue_with_maxsize.maxsize == 10
    
    async def test_typed_queue_put_get(self):
        """Test putting and getting items from the queue."""
        queue = TypedQueue[str]()
        
        # Put an item
        await queue.put("test_item")
        
        # Get the item
        item = await queue.get()
        assert item == "test_item"
        
        # Mark the task as done
        queue.task_done()
        
        # Queue should be empty now
        assert queue.empty()
    
    async def test_typed_queue_with_message(self):
        """Test using TypedQueue with Message objects."""
        queue = TypedQueue[Message]()
        
        # Create a message
        msg = Message(topic="test/topic", data={"key": "value"})
        
        # Put the message in the queue
        await queue.put(msg)
        
        # Get the message from the queue
        retrieved_msg = await queue.get()
        
        # Verify it's the same message
        assert retrieved_msg.topic == "test/topic"
        assert retrieved_msg.data == {"key": "value"}
        
        # Mark the task as done
        queue.task_done()


class MockTransport(Transport):
    """Mock implementation of the Transport interface for testing."""
    
    def __init__(self):
        self.connected = False
        self.subscriptions = {}
        self.published_messages = []
        self.requests = []
        self.responses = []
    
    async def connect(self):
        self.connected = True
        return True
    
    async def close(self):
        self.connected = False
        return True
    
    async def publish(self, topic: str, data: Any, headers: Dict[str, str] = None):
        if not self.connected:
            raise ConnectionError("Not connected")
        
        message = Message(topic=topic, data=data, headers=headers)
        self.published_messages.append(message)
        return True
    
    async def subscribe(self, topic: str, handler):
        if not self.connected:
            raise ConnectionError("Not connected")
        
        subscription_id = f"sub_{len(self.subscriptions)}"
        self.subscriptions[subscription_id] = (topic, handler)
        return subscription_id
    
    async def unsubscribe(self, subscription_id: str):
        if not self.connected:
            raise ConnectionError("Not connected")
        
        if subscription_id in self.subscriptions:
            del self.subscriptions[subscription_id]
            return True
        return False
    
    async def request(self, topic: str, data: Any, timeout: float = 5.0, headers: Dict[str, str] = None):
        if not self.connected:
            raise ConnectionError("Not connected")
        
        request_message = Message(topic=topic, data=data, headers=headers)
        self.requests.append(request_message)
        
        # Simulate a response
        response_message = Message(
            topic=f"response_{topic}",
            data={"response": "test_response"},
            headers={"correlation-id": "test_id"}
        )
        return response_message
    
    async def respond(self, request_message: Message, data: Any, headers: Dict[str, str] = None):
        if not self.connected:
            raise ConnectionError("Not connected")
        
        if not request_message.reply:
            raise ValueError("Request message has no reply topic")
        
        response_message = Message(
            topic=request_message.reply,
            data=data,
            headers=headers
        )
        self.responses.append(response_message)
        return True


class TestTransport:
    """Tests for the Transport interface."""
    
    @pytest.fixture
    def mock_transport(self):
        """Create a mock transport for testing."""
        return MockTransport()
    
    async def test_connect(self, mock_transport):
        """Test connecting to the transport."""
        assert not mock_transport.connected
        
        result = await mock_transport.connect()
        
        assert result is True
        assert mock_transport.connected
    
    async def test_close(self, mock_transport):
        """Test closing the transport connection."""
        # First connect
        await mock_transport.connect()
        assert mock_transport.connected
        
        # Then close
        result = await mock_transport.close()
        
        assert result is True
        assert not mock_transport.connected
    
    async def test_publish(self, mock_transport):
        """Test publishing a message."""
        # First connect
        await mock_transport.connect()
        
        # Publish a message
        result = await mock_transport.publish(
            topic="test/topic",
            data={"key": "value"},
            headers={"content-type": "application/json"}
        )
        
        assert result is True
        assert len(mock_transport.published_messages) == 1
        
        published_msg = mock_transport.published_messages[0]
        assert published_msg.topic == "test/topic"
        assert published_msg.data == {"key": "value"}
        assert published_msg.headers == {"content-type": "application/json"}
    
    async def test_publish_not_connected(self, mock_transport):
        """Test publishing when not connected raises an error."""
        with pytest.raises(ConnectionError):
            await mock_transport.publish("test/topic", "test_data")
    
    async def test_subscribe(self, mock_transport):
        """Test subscribing to a topic."""
        # First connect
        await mock_transport.connect()
        
        # Create a handler
        async def handler(message):
            pass
        
        # Subscribe to a topic
        subscription_id = await mock_transport.subscribe("test/topic", handler)
        
        assert subscription_id is not None
        assert subscription_id in mock_transport.subscriptions
        assert mock_transport.subscriptions[subscription_id][0] == "test/topic"
    
    async def test_subscribe_not_connected(self, mock_transport):
        """Test subscribing when not connected raises an error."""
        async def handler(message):
            pass
        
        with pytest.raises(ConnectionError):
            await mock_transport.subscribe("test/topic", handler)
    
    async def test_unsubscribe(self, mock_transport):
        """Test unsubscribing from a topic."""
        # First connect
        await mock_transport.connect()
        
        # Subscribe to a topic
        async def handler(message):
            pass
        
        subscription_id = await mock_transport.subscribe("test/topic", handler)
        
        # Unsubscribe
        result = await mock_transport.unsubscribe(subscription_id)
        
        assert result is True
        assert subscription_id not in mock_transport.subscriptions
    
    async def test_unsubscribe_not_connected(self, mock_transport):
        """Test unsubscribing when not connected raises an error."""
        with pytest.raises(ConnectionError):
            await mock_transport.unsubscribe("non_existent_id")
    
    async def test_request(self, mock_transport):
        """Test sending a request and receiving a response."""
        # First connect
        await mock_transport.connect()
        
        # Send a request
        response = await mock_transport.request(
            topic="test/request",
            data={"request": "test_request"},
            timeout=2.0,
            headers={"content-type": "application/json"}
        )
        
        assert len(mock_transport.requests) == 1
        request_msg = mock_transport.requests[0]
        assert request_msg.topic == "test/request"
        assert request_msg.data == {"request": "test_request"}
        
        # Check the response
        assert response.topic == "response_test/request"
        assert response.data == {"response": "test_response"}
    
    async def test_request_not_connected(self, mock_transport):
        """Test sending a request when not connected raises an error."""
        with pytest.raises(ConnectionError):
            await mock_transport.request("test/topic", "test_data")
    
    async def test_respond(self, mock_transport):
        """Test responding to a request."""
        # First connect
        await mock_transport.connect()
        
        # Create a request message with a reply topic
        request_msg = Message(
            topic="test/request",
            data={"request": "test_request"},
            reply="test/reply"
        )
        
        # Send a response
        result = await mock_transport.respond(
            request_message=request_msg,
            data={"response": "test_response"},
            headers={"content-type": "application/json"}
        )
        
        assert result is True
        assert len(mock_transport.responses) == 1
        
        response_msg = mock_transport.responses[0]
        assert response_msg.topic == "test/reply"
        assert response_msg.data == {"response": "test_response"}
        assert response_msg.headers == {"content-type": "application/json"}
    
    async def test_respond_no_reply_topic(self, mock_transport):
        """Test responding to a request with no reply topic raises an error."""
        # First connect
        await mock_transport.connect()
        
        # Create a request message without a reply topic
        request_msg = Message(
            topic="test/request",
            data={"request": "test_request"}
        )
        
        # Attempt to send a response
        with pytest.raises(ValueError):
            await mock_transport.respond(
                request_message=request_msg,
                data={"response": "test_response"}
            )
    
    async def test_respond_not_connected(self, mock_transport):
        """Test responding when not connected raises an error."""
        request_msg = Message(
            topic="test/request",
            data={"request": "test_request"},
            reply="test/reply"
        )
        
        with pytest.raises(ConnectionError):
            await mock_transport.respond(request_msg, "test_data") 