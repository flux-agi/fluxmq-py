"""
Tests for the SyncNats adapter.
"""
import pytest
import threading
import time
from unittest.mock import MagicMock, patch
import json

from fluxmq.adapter.nats import SyncNats
from fluxmq.message import Message


@pytest.fixture
def mock_sync_nats_client():
    """Create a mock NATS client for synchronous operations."""
    mock_client = MagicMock()
    mock_client.connect = MagicMock(return_value=None)
    mock_client.close = MagicMock(return_value=None)
    mock_client.publish = MagicMock(return_value=None)
    mock_client.subscribe = MagicMock(return_value=MagicMock())
    mock_client.unsubscribe = MagicMock(return_value=None)
    mock_client.request = MagicMock(return_value=MagicMock(
        data=json.dumps({"response": "test_response"}).encode(),
        subject="response_subject",
        reply="reply_subject",
        headers={"correlation-id": "test_id"}
    ))
    return mock_client


class TestSyncNats:
    """Tests for the SyncNats class."""
    
    def test_init(self):
        """Test SyncNats initialization."""
        sync_nats = SyncNats(servers=["nats://localhost:4222"])
        
        assert sync_nats._servers == ["nats://localhost:4222"]
        assert sync_nats._nc is None
        assert sync_nats._subscriptions == {}
        assert sync_nats._lock is not None
        assert isinstance(sync_nats._lock, threading.RLock)
    
    def test_connect(self, mock_sync_nats_client):
        """Test connecting to NATS."""
        with patch("fluxmq.adapter.nats.NATS", return_value=mock_sync_nats_client):
            sync_nats = SyncNats(servers=["nats://localhost:4222"])
            
            result = sync_nats.connect()
            
            assert result is True
            mock_sync_nats_client.connect.assert_called_once()
    
    def test_connect_with_error(self, mock_sync_nats_client):
        """Test connecting to NATS with an error."""
        mock_sync_nats_client.connect.side_effect = Exception("Connection error")
        
        with patch("fluxmq.adapter.nats.NATS", return_value=mock_sync_nats_client):
            sync_nats = SyncNats(servers=["nats://localhost:4222"])
            
            with pytest.raises(ConnectionError):
                sync_nats.connect()
    
    def test_close(self, mock_sync_nats_client):
        """Test closing the NATS connection."""
        with patch("fluxmq.adapter.nats.NATS", return_value=mock_sync_nats_client):
            sync_nats = SyncNats(servers=["nats://localhost:4222"])
            sync_nats._nc = mock_sync_nats_client  # Manually set the client
            
            result = sync_nats.close()
            
            assert result is True
            mock_sync_nats_client.close.assert_called_once()
    
    def test_publish(self, mock_sync_nats_client):
        """Test publishing a message to NATS."""
        with patch("fluxmq.adapter.nats.NATS", return_value=mock_sync_nats_client):
            sync_nats = SyncNats(servers=["nats://localhost:4222"])
            sync_nats._nc = mock_sync_nats_client  # Manually set the client
            
            result = sync_nats.publish(
                topic="test.topic",
                data={"key": "value"},
                headers={"content-type": "application/json"}
            )
            
            assert result is True
            mock_sync_nats_client.publish.assert_called_once()
            # Check that the data was properly serialized
            call_args = mock_sync_nats_client.publish.call_args[0]
            assert call_args[0] == "test.topic"  # subject
            assert json.loads(call_args[1].decode()) == {"key": "value"}  # data
    
    def test_publish_not_connected(self, mock_sync_nats_client):
        """Test publishing when not connected raises an error."""
        with patch("fluxmq.adapter.nats.NATS", return_value=mock_sync_nats_client):
            sync_nats = SyncNats(servers=["nats://localhost:4222"])
            
            with pytest.raises(ConnectionError):
                sync_nats.publish("test.topic", "test_data")
    
    def test_subscribe(self, mock_sync_nats_client):
        """Test subscribing to a NATS topic."""
        with patch("fluxmq.adapter.nats.NATS", return_value=mock_sync_nats_client):
            sync_nats = SyncNats(servers=["nats://localhost:4222"])
            sync_nats._nc = mock_sync_nats_client  # Manually set the client
            
            # Create a handler
            def handler(message):
                pass
            
            subscription_id = sync_nats.subscribe("test.topic", handler)
            
            assert subscription_id is not None
            mock_sync_nats_client.subscribe.assert_called_once()
    
    def test_subscribe_not_connected(self, mock_sync_nats_client):
        """Test subscribing when not connected raises an error."""
        with patch("fluxmq.adapter.nats.NATS", return_value=mock_sync_nats_client):
            sync_nats = SyncNats(servers=["nats://localhost:4222"])
            
            def handler(message):
                pass
            
            with pytest.raises(ConnectionError):
                sync_nats.subscribe("test.topic", handler)
    
    def test_unsubscribe(self, mock_sync_nats_client):
        """Test unsubscribing from a NATS topic."""
        with patch("fluxmq.adapter.nats.NATS", return_value=mock_sync_nats_client):
            sync_nats = SyncNats(servers=["nats://localhost:4222"])
            sync_nats._nc = mock_sync_nats_client  # Manually set the client
            
            # Create a mock subscription
            mock_subscription = MagicMock()
            sync_nats._subscriptions["test_sub"] = mock_subscription
            
            result = sync_nats.unsubscribe("test_sub")
            
            assert result is True
            mock_subscription.unsubscribe.assert_called_once()
    
    def test_unsubscribe_not_connected(self, mock_sync_nats_client):
        """Test unsubscribing when not connected raises an error."""
        with patch("fluxmq.adapter.nats.NATS", return_value=mock_sync_nats_client):
            sync_nats = SyncNats(servers=["nats://localhost:4222"])
            
            with pytest.raises(ConnectionError):
                sync_nats.unsubscribe("non_existent_id")
    
    def test_request(self, mock_sync_nats_client):
        """Test sending a request to NATS and receiving a response."""
        with patch("fluxmq.adapter.nats.NATS", return_value=mock_sync_nats_client):
            sync_nats = SyncNats(servers=["nats://localhost:4222"])
            sync_nats._nc = mock_sync_nats_client  # Manually set the client
            
            response = sync_nats.request(
                topic="test.request",
                data={"request": "test_request"},
                timeout=2.0,
                headers={"content-type": "application/json"}
            )
            
            assert response is not None
            assert response.data == {"response": "test_response"}
            mock_sync_nats_client.request.assert_called_once()
    
    def test_request_not_connected(self, mock_sync_nats_client):
        """Test sending a request when not connected raises an error."""
        with patch("fluxmq.adapter.nats.NATS", return_value=mock_sync_nats_client):
            sync_nats = SyncNats(servers=["nats://localhost:4222"])
            
            with pytest.raises(ConnectionError):
                sync_nats.request("test.topic", "test_data")
    
    def test_respond(self, mock_sync_nats_client):
        """Test responding to a NATS request."""
        with patch("fluxmq.adapter.nats.NATS", return_value=mock_sync_nats_client):
            sync_nats = SyncNats(servers=["nats://localhost:4222"])
            sync_nats._nc = mock_sync_nats_client  # Manually set the client
            
            # Create a request message with a reply topic
            request_msg = Message(
                topic="test.request",
                data={"request": "test_request"},
                reply="test.reply"
            )
            
            result = sync_nats.respond(
                request_message=request_msg,
                data={"response": "test_response"},
                headers={"content-type": "application/json"}
            )
            
            assert result is True
            mock_sync_nats_client.publish.assert_called_once()
            # Check that the response was sent to the correct reply subject
            call_args = mock_sync_nats_client.publish.call_args[0]
            assert call_args[0] == "test.reply"  # subject
    
    def test_respond_no_reply_topic(self, mock_sync_nats_client):
        """Test responding to a request with no reply topic raises an error."""
        with patch("fluxmq.adapter.nats.NATS", return_value=mock_sync_nats_client):
            sync_nats = SyncNats(servers=["nats://localhost:4222"])
            sync_nats._nc = mock_sync_nats_client  # Manually set the client
            
            # Create a request message without a reply topic
            request_msg = Message(
                topic="test.request",
                data={"request": "test_request"}
            )
            
            with pytest.raises(ValueError):
                sync_nats.respond(
                    request_message=request_msg,
                    data={"response": "test_response"}
                )
    
    def test_respond_not_connected(self, mock_sync_nats_client):
        """Test responding when not connected raises an error."""
        with patch("fluxmq.adapter.nats.NATS", return_value=mock_sync_nats_client):
            sync_nats = SyncNats(servers=["nats://localhost:4222"])
            
            request_msg = Message(
                topic="test.request",
                data={"request": "test_request"},
                reply="test.reply"
            )
            
            with pytest.raises(ConnectionError):
                sync_nats.respond(request_msg, "test_data")
    
    def test_thread_safety(self, mock_sync_nats_client):
        """Test thread safety of the SyncNats implementation."""
        with patch("fluxmq.adapter.nats.NATS", return_value=mock_sync_nats_client):
            sync_nats = SyncNats(servers=["nats://localhost:4222"])
            sync_nats._nc = mock_sync_nats_client  # Manually set the client
            
            # Create a list to store any exceptions that occur in threads
            exceptions = []
            
            def publish_messages():
                try:
                    for i in range(10):
                        sync_nats.publish(f"test.topic.{i}", f"data_{i}")
                        time.sleep(0.01)  # Small delay to increase chance of thread interleaving
                except Exception as e:
                    exceptions.append(e)
            
            def subscribe_and_unsubscribe():
                try:
                    subscription_ids = []
                    for i in range(5):
                        def handler(message):
                            pass
                        
                        subscription_id = sync_nats.subscribe(f"test.topic.{i}", handler)
                        subscription_ids.append(subscription_id)
                        time.sleep(0.01)  # Small delay
                    
                    for subscription_id in subscription_ids:
                        sync_nats.unsubscribe(subscription_id)
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
            
            # Verify the expected number of publish calls
            assert mock_sync_nats_client.publish.call_count == 10 