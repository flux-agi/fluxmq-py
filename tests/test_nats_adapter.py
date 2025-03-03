"""
Tests for the NATS adapter.
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import json

from fluxmq.adapter.nats import Nats, SyncNats, NatsTopic, NatsStatus
from fluxmq.message import Message


@pytest.fixture
def mock_nats_client():
    """Create a mock NATS client."""
    mock_client = AsyncMock()
    mock_client.connect = AsyncMock(return_value=None)
    mock_client.close = AsyncMock(return_value=None)
    mock_client.publish = AsyncMock(return_value=None)
    mock_client.subscribe = AsyncMock(return_value=MagicMock())
    mock_client.unsubscribe = AsyncMock(return_value=None)
    mock_client.request = AsyncMock(return_value=MagicMock(
        data=json.dumps({"response": "test_response"}).encode(),
        subject="response_subject",
        reply="reply_subject",
        headers={"correlation-id": "test_id"}
    ))
    return mock_client


class TestNats:
    """Tests for the Nats class."""
    
    @pytest.mark.asyncio
    async def test_connect(self, mock_nats_client):
        """Test connecting to NATS."""
        with patch("fluxmq.adapter.nats.NATS.connect", new=mock_nats_client.connect):
            nats = Nats(servers=["nats://localhost:4222"])
            result = await nats.connect()
            
            assert result is True
            mock_nats_client.connect.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_connect_with_error(self, mock_nats_client):
        """Test connecting to NATS with an error."""
        mock_nats_client.connect.side_effect = Exception("Connection error")
        
        with patch("fluxmq.adapter.nats.NATS.connect", new=mock_nats_client.connect):
            nats = Nats(servers=["nats://localhost:4222"])
            
            with pytest.raises(ConnectionError):
                await nats.connect()
    
    @pytest.mark.asyncio
    async def test_close(self, mock_nats_client):
        """Test closing the NATS connection."""
        with patch("fluxmq.adapter.nats.NATS", return_value=mock_nats_client):
            nats = Nats(servers=["nats://localhost:4222"])
            nats._nc = mock_nats_client  # Manually set the client
            
            result = await nats.close()
            
            assert result is True
            mock_nats_client.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_publish(self, mock_nats_client):
        """Test publishing a message to NATS."""
        with patch("fluxmq.adapter.nats.NATS", return_value=mock_nats_client):
            nats = Nats(servers=["nats://localhost:4222"])
            nats._nc = mock_nats_client  # Manually set the client
            
            result = await nats.publish(
                topic="test.topic",
                data={"key": "value"},
                headers={"content-type": "application/json"}
            )
            
            assert result is True
            mock_nats_client.publish.assert_called_once()
            # Check that the data was properly serialized
            call_args = mock_nats_client.publish.call_args[0]
            assert call_args[0] == "test.topic"  # subject
            assert json.loads(call_args[1].decode()) == {"key": "value"}  # data
    
    @pytest.mark.asyncio
    async def test_subscribe(self, mock_nats_client):
        """Test subscribing to a NATS topic."""
        with patch("fluxmq.adapter.nats.NATS", return_value=mock_nats_client):
            nats = Nats(servers=["nats://localhost:4222"])
            nats._nc = mock_nats_client  # Manually set the client
            
            # Create a handler
            async def handler(message):
                pass
            
            subscription_id = await nats.subscribe("test.topic", handler)
            
            assert subscription_id is not None
            mock_nats_client.subscribe.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_unsubscribe(self, mock_nats_client):
        """Test unsubscribing from a NATS topic."""
        with patch("fluxmq.adapter.nats.NATS", return_value=mock_nats_client):
            nats = Nats(servers=["nats://localhost:4222"])
            nats._nc = mock_nats_client  # Manually set the client
            
            # Create a mock subscription
            mock_subscription = MagicMock()
            nats._subscriptions["test_sub"] = mock_subscription
            
            result = await nats.unsubscribe("test_sub")
            
            assert result is True
            mock_subscription.unsubscribe.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_request(self, mock_nats_client):
        """Test sending a request to NATS and receiving a response."""
        with patch("fluxmq.adapter.nats.NATS", return_value=mock_nats_client):
            nats = Nats(servers=["nats://localhost:4222"])
            nats._nc = mock_nats_client  # Manually set the client
            
            response = await nats.request(
                topic="test.request",
                data={"request": "test_request"},
                timeout=2.0,
                headers={"content-type": "application/json"}
            )
            
            assert response is not None
            assert response.data == {"response": "test_response"}
            mock_nats_client.request.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_respond(self, mock_nats_client):
        """Test responding to a NATS request."""
        with patch("fluxmq.adapter.nats.NATS", return_value=mock_nats_client):
            nats = Nats(servers=["nats://localhost:4222"])
            nats._nc = mock_nats_client  # Manually set the client
            
            # Create a request message with a reply topic
            request_msg = Message(
                topic="test.request",
                data={"request": "test_request"},
                reply="test.reply"
            )
            
            result = await nats.respond(
                request_message=request_msg,
                data={"response": "test_response"},
                headers={"content-type": "application/json"}
            )
            
            assert result is True
            mock_nats_client.publish.assert_called_once()
            # Check that the response was sent to the correct reply subject
            call_args = mock_nats_client.publish.call_args[0]
            assert call_args[0] == "test.reply"  # subject


class TestNatsTopic:
    """Tests for the NatsTopic class."""
    
    def test_topic_conversion(self):
        """Test that NatsTopic correctly converts dot notation."""
        topic = NatsTopic(prefix="test")
        
        # Test status topic
        status_topic = topic.status("service1")
        assert status_topic == "test.status.service1"
        
        # Test service state topics
        set_state_topic = topic.set_service_state("service1")
        assert set_state_topic == "test.state.service1"
        
        get_state_topic = topic.get_service_state("service1")
        assert get_state_topic == "test.state.service1"
        
        # Test common data topics
        set_data_topic = topic.set_common_data("key1")
        assert set_data_topic == "test.common.key1"
        
        get_data_topic = topic.get_common_data("key1")
        assert get_data_topic == "test.common.key1"
        
        # Test command topics
        service_cmd_topic = topic.service_command("service1", "start")
        assert service_cmd_topic == "test.command.service1.start"
        
        node_cmd_topic = topic.node_command("node1", "restart")
        assert node_cmd_topic == "test.node.node1.restart"
        
        # Test node status topic
        node_status_topic = topic.node_status("node1")
        assert node_status_topic == "test.node.node1.status"


class TestNatsStatus:
    """Tests for the NatsStatus class."""
    
    def test_status_methods(self):
        """Test the status methods of NatsStatus."""
        status = NatsStatus()
        
        # Test status methods
        assert status.unknown() == "unknown"
        assert status.starting() == "starting"
        assert status.running() == "running"
        assert status.stopping() == "stopping"
        assert status.stopped() == "stopped"
        assert status.error() == "error"
        assert status.error("Test error") == "error"
        
        # Test as_dict method
        status.error("Test error message")
        result = status.as_dict()
        assert result == {"status": "error", "message": "Test error message"} 