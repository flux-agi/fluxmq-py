"""
Tests for the SyncTransport interface.
"""
import pytest
import threading
import time
from unittest.mock import MagicMock
from typing import Any, Dict, Optional

from fluxmq.transport import SyncTransport
from fluxmq.message import Message


class MockSyncTransport(SyncTransport):
    """Mock implementation of the SyncTransport interface for testing."""
    
    def __init__(self):
        self.connected = False
        self.subscriptions = {}
        self.published_messages = []
        self.requests = []
        self.responses = []
        self._lock = threading.Lock()
    
    def connect(self):
        with self._lock:
            self.connected = True
        return True
    
    def close(self):
        with self._lock:
            self.connected = False
        return True
    
    def publish(self, topic: str, data: Any, headers: Optional[Dict[str, str]] = None):
        with self._lock:
            if not self.connected:
                raise ConnectionError("Not connected")
            
            message = Message(topic=topic, data=data, headers=headers)
            self.published_messages.append(message)
        return True
    
    def subscribe(self, topic: str, handler):
        with self._lock:
            if not self.connected:
                raise ConnectionError("Not connected")
            
            subscription_id = f"sub_{len(self.subscriptions)}"
            self.subscriptions[subscription_id] = (topic, handler)
        return subscription_id
    
    def unsubscribe(self, subscription_id: str):
        with self._lock:
            if not self.connected:
                raise ConnectionError("Not connected")
            
            if subscription_id in self.subscriptions:
                del self.subscriptions[subscription_id]
                return True
        return False
    
    def request(self, topic: str, data: Any, timeout: float = 5.0, headers: Optional[Dict[str, str]] = None):
        with self._lock:
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
    
    def respond(self, request_message: Message, data: Any, headers: Optional[Dict[str, str]] = None):
        with self._lock:
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


class TestSyncTransport:
    """Tests for the SyncTransport interface."""
    
    @pytest.fixture
    def mock_sync_transport(self):
        """Create a mock sync transport for testing."""
        return MockSyncTransport()
    
    def test_connect(self, mock_sync_transport):
        """Test connecting to the transport."""
        assert not mock_sync_transport.connected
        
        result = mock_sync_transport.connect()
        
        assert result is True
        assert mock_sync_transport.connected
    
    def test_close(self, mock_sync_transport):
        """Test closing the transport connection."""
        # First connect
        mock_sync_transport.connect()
        assert mock_sync_transport.connected
        
        # Then close
        result = mock_sync_transport.close()
        
        assert result is True
        assert not mock_sync_transport.connected
    
    def test_publish(self, mock_sync_transport):
        """Test publishing a message."""
        # First connect
        mock_sync_transport.connect()
        
        # Publish a message
        result = mock_sync_transport.publish(
            topic="test/topic",
            data={"key": "value"},
            headers={"content-type": "application/json"}
        )
        
        assert result is True
        assert len(mock_sync_transport.published_messages) == 1
        
        published_msg = mock_sync_transport.published_messages[0]
        assert published_msg.topic == "test/topic"
        assert published_msg.payload == {"key": "value"}
        assert published_msg.headers == {"content-type": "application/json"}
    
    def test_publish_not_connected(self, mock_sync_transport):
        """Test publishing when not connected raises an error."""
        with pytest.raises(ConnectionError):
            mock_sync_transport.publish("test/topic", "test_data")
    
    def test_subscribe(self, mock_sync_transport):
        """Test subscribing to a topic."""
        # First connect
        mock_sync_transport.connect()
        
        # Create a handler
        def handler(message):
            pass
        
        # Subscribe to a topic
        subscription_id = mock_sync_transport.subscribe("test/topic", handler)
        
        assert subscription_id is not None
        assert subscription_id in mock_sync_transport.subscriptions
        assert mock_sync_transport.subscriptions[subscription_id][0] == "test/topic"
    
    def test_subscribe_not_connected(self, mock_sync_transport):
        """Test subscribing when not connected raises an error."""
        def handler(message):
            pass
        
        with pytest.raises(ConnectionError):
            mock_sync_transport.subscribe("test/topic", handler)
    
    def test_unsubscribe(self, mock_sync_transport):
        """Test unsubscribing from a topic."""
        # First connect
        mock_sync_transport.connect()
        
        # Subscribe to a topic
        def handler(message):
            pass
        
        subscription_id = mock_sync_transport.subscribe("test/topic", handler)
        
        # Unsubscribe
        result = mock_sync_transport.unsubscribe(subscription_id)
        
        assert result is True
        assert subscription_id not in mock_sync_transport.subscriptions
    
    def test_unsubscribe_not_connected(self, mock_sync_transport):
        """Test unsubscribing when not connected raises an error."""
        with pytest.raises(ConnectionError):
            mock_sync_transport.unsubscribe("non_existent_id")
    
    def test_request(self, mock_sync_transport):
        """Test sending a request and receiving a response."""
        # First connect
        mock_sync_transport.connect()
        
        # Send a request
        response = mock_sync_transport.request(
            topic="test/request",
            data={"request": "test_request"},
            timeout=2.0,
            headers={"content-type": "application/json"}
        )
        
        assert len(mock_sync_transport.requests) == 1
        request_msg = mock_sync_transport.requests[0]
        assert request_msg.topic == "test/request"
        assert request_msg.payload == {"request": "test_request"}
        
        # Check the response
        assert response.topic == "response_test/request"
        assert response.payload == {"response": "test_response"}
    
    def test_request_not_connected(self, mock_sync_transport):
        """Test sending a request when not connected raises an error."""
        with pytest.raises(ConnectionError):
            mock_sync_transport.request("test/topic", "test_data")
    
    def test_respond(self, mock_sync_transport):
        """Test responding to a request."""
        # First connect
        mock_sync_transport.connect()
        
        # Create a request message with a reply topic
        request_msg = Message(
            topic="test/request",
            data={"request": "test_request"},
            reply="test/reply"
        )
        
        # Send a response
        result = mock_sync_transport.respond(
            request_message=request_msg,
            data={"response": "test_response"},
            headers={"content-type": "application/json"}
        )
        
        assert result is True
        assert len(mock_sync_transport.responses) == 1
        
        response_msg = mock_sync_transport.responses[0]
        assert response_msg.topic == "test/reply"
        assert response_msg.payload == {"response": "test_response"}
        assert response_msg.headers == {"content-type": "application/json"}
    
    def test_respond_no_reply_topic(self, mock_sync_transport):
        """Test responding to a request with no reply topic raises an error."""
        # First connect
        mock_sync_transport.connect()
        
        # Create a request message without a reply topic
        request_msg = Message(
            topic="test/request",
            data={"request": "test_request"}
        )
        
        # Attempt to send a response
        with pytest.raises(ValueError):
            mock_sync_transport.respond(
                request_message=request_msg,
                data={"response": "test_response"}
            )
    
    def test_respond_not_connected(self, mock_sync_transport):
        """Test responding when not connected raises an error."""
        request_msg = Message(
            topic="test/request",
            data={"request": "test_request"},
            reply="test/reply"
        )
        
        with pytest.raises(ConnectionError):
            mock_sync_transport.respond(request_msg, "test_data")
    
    def test_thread_safety(self, mock_sync_transport):
        """Test thread safety of the SyncTransport implementation."""
        # First connect
        mock_sync_transport.connect()
        
        # Create a list to store any exceptions that occur in threads
        exceptions = []
        
        def publish_messages():
            try:
                for i in range(10):
                    mock_sync_transport.publish(f"test/topic/{i}", f"data_{i}")
                    time.sleep(0.01)  # Small delay to increase chance of thread interleaving
            except Exception as e:
                exceptions.append(e)
        
        def subscribe_and_unsubscribe():
            try:
                subscription_ids = []
                for i in range(5):
                    def handler(message):
                        pass
                    
                    subscription_id = mock_sync_transport.subscribe(f"test/topic/{i}", handler)
                    subscription_ids.append(subscription_id)
                    time.sleep(0.01)  # Small delay
                
                for subscription_id in subscription_ids:
                    mock_sync_transport.unsubscribe(subscription_id)
                    time.sleep(0.01)  # Small delay
            except Exception as e:
                exceptions.append(e)
        
        # Create and start threads
        thread1 = threading.Thread(target=publish_messages)
        thread2 = threading.Thread(target=subscribe_and_unsubscribe)
        
        thread1.start()
        thread2.start()
        
        # Wait for threads to complete
        thread1.join()
        thread2.join()
        
        # Check if any exceptions occurred
        assert not exceptions, f"Exceptions occurred during thread execution: {exceptions}"
        
        # Verify the expected number of published messages
        assert len(mock_sync_transport.published_messages) == 10 