"""
Tests for the MQTT adapter.
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, call
import json
import uuid

from fluxmq.adapter.mqtt import MQTT, MQTTTopic, MQTTStatus
from fluxmq.message import Message


@pytest.fixture
def mock_mqtt_client():
    """Create a mock MQTT client."""
    mock_client = MagicMock()
    mock_client.connect = AsyncMock(return_value=None)
    mock_client.disconnect = AsyncMock(return_value=None)
    mock_client.publish = AsyncMock(return_value=MagicMock(
        is_published=AsyncMock(return_value=True)
    ))
    mock_client.subscribe = AsyncMock(return_value=MagicMock(
        mid=1
    ))
    mock_client.unsubscribe = AsyncMock(return_value=MagicMock(
        mid=2
    ))
    return mock_client


class TestMQTT:
    """Tests for the MQTT class."""
    
    @pytest.mark.asyncio
    async def test_init(self):
        """Test MQTT initialization."""
        mqtt = MQTT(host="localhost", port=1883, client_id="test_client")
        
        assert mqtt._host == "localhost"
        assert mqtt._port == 1883
        assert mqtt._client_id == "test_client"
        assert mqtt._username is None
        assert mqtt._password is None
        assert mqtt._connected is False
        assert mqtt._subscriptions == {}
        assert mqtt._response_futures == {}
    
    @pytest.mark.asyncio
    async def test_connect(self, mock_mqtt_client):
        """Test connecting to MQTT broker."""
        with patch("fluxmq.adapter.mqtt.Client", return_value=mock_mqtt_client):
            mqtt = MQTT(host="localhost", port=1883)
            
            # Mock the _on_connect callback
            async def mock_on_connect(client, userdata, flags, rc):
                mqtt._connected = True
            
            mqtt._on_connect = mock_on_connect
            
            result = await mqtt.connect()
            
            assert result is True
            mock_mqtt_client.connect.assert_called_once_with("localhost", 1883)
            mock_mqtt_client.connect_async.assert_called_once()
            mock_mqtt_client.loop_start.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_connect_with_auth(self, mock_mqtt_client):
        """Test connecting to MQTT broker with authentication."""
        with patch("fluxmq.adapter.mqtt.Client", return_value=mock_mqtt_client):
            mqtt = MQTT(
                host="localhost",
                port=1883,
                username="test_user",
                password="test_pass"
            )
            
            # Mock the _on_connect callback
            async def mock_on_connect(client, userdata, flags, rc):
                mqtt._connected = True
            
            mqtt._on_connect = mock_on_connect
            
            result = await mqtt.connect()
            
            assert result is True
            mock_mqtt_client.username_pw_set.assert_called_once_with("test_user", "test_pass")
    
    @pytest.mark.asyncio
    async def test_connect_error(self, mock_mqtt_client):
        """Test connecting to MQTT broker with error."""
        mock_mqtt_client.connect.side_effect = Exception("Connection error")
        
        with patch("fluxmq.adapter.mqtt.Client", return_value=mock_mqtt_client):
            mqtt = MQTT(host="localhost", port=1883)
            
            with pytest.raises(ConnectionError):
                await mqtt.connect()
    
    @pytest.mark.asyncio
    async def test_close(self, mock_mqtt_client):
        """Test closing the MQTT connection."""
        with patch("fluxmq.adapter.mqtt.Client", return_value=mock_mqtt_client):
            mqtt = MQTT(host="localhost", port=1883)
            mqtt._client = mock_mqtt_client
            mqtt._connected = True
            
            result = await mqtt.close()
            
            assert result is True
            assert mqtt._connected is False
            mock_mqtt_client.loop_stop.assert_called_once()
            mock_mqtt_client.disconnect.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_publish(self, mock_mqtt_client):
        """Test publishing a message to MQTT."""
        with patch("fluxmq.adapter.mqtt.Client", return_value=mock_mqtt_client):
            mqtt = MQTT(host="localhost", port=1883)
            mqtt._client = mock_mqtt_client
            mqtt._connected = True
            
            result = await mqtt.publish(
                topic="test/topic",
                data={"key": "value"},
                headers={"content-type": "application/json"}
            )
            
            assert result is True
            mock_mqtt_client.publish.assert_called_once()
            # Check that the data was properly serialized
            call_args = mock_mqtt_client.publish.call_args[0]
            assert call_args[0] == "test/topic"  # topic
            assert json.loads(call_args[1].decode()) == {"key": "value"}  # payload
    
    @pytest.mark.asyncio
    async def test_publish_not_connected(self, mock_mqtt_client):
        """Test publishing when not connected raises an error."""
        with patch("fluxmq.adapter.mqtt.Client", return_value=mock_mqtt_client):
            mqtt = MQTT(host="localhost", port=1883)
            mqtt._client = mock_mqtt_client
            mqtt._connected = False
            
            with pytest.raises(ConnectionError):
                await mqtt.publish("test/topic", "test_data")
    
    @pytest.mark.asyncio
    async def test_subscribe(self, mock_mqtt_client):
        """Test subscribing to an MQTT topic."""
        with patch("fluxmq.adapter.mqtt.Client", return_value=mock_mqtt_client):
            mqtt = MQTT(host="localhost", port=1883)
            mqtt._client = mock_mqtt_client
            mqtt._connected = True
            
            # Create a handler
            async def handler(message):
                pass
            
            subscription_id = await mqtt.subscribe("test/topic", handler)
            
            assert subscription_id is not None
            assert "test/topic" in mqtt._subscriptions
            mock_mqtt_client.subscribe.assert_called_once_with("test/topic")
            mock_mqtt_client.message_callback_add.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_subscribe_not_connected(self, mock_mqtt_client):
        """Test subscribing when not connected raises an error."""
        with patch("fluxmq.adapter.mqtt.Client", return_value=mock_mqtt_client):
            mqtt = MQTT(host="localhost", port=1883)
            mqtt._client = mock_mqtt_client
            mqtt._connected = False
            
            async def handler(message):
                pass
            
            with pytest.raises(ConnectionError):
                await mqtt.subscribe("test/topic", handler)
    
    @pytest.mark.asyncio
    async def test_unsubscribe(self, mock_mqtt_client):
        """Test unsubscribing from an MQTT topic."""
        with patch("fluxmq.adapter.mqtt.Client", return_value=mock_mqtt_client):
            mqtt = MQTT(host="localhost", port=1883)
            mqtt._client = mock_mqtt_client
            mqtt._connected = True
            
            # Add a subscription
            mqtt._subscriptions["test/topic"] = {
                "handler": AsyncMock(),
                "callback_id": "callback_1"
            }
            
            result = await mqtt.unsubscribe("test/topic")
            
            assert result is True
            assert "test/topic" not in mqtt._subscriptions
            mock_mqtt_client.unsubscribe.assert_called_once_with("test/topic")
            mock_mqtt_client.message_callback_remove.assert_called_once_with("callback_1")
    
    @pytest.mark.asyncio
    async def test_unsubscribe_not_connected(self, mock_mqtt_client):
        """Test unsubscribing when not connected raises an error."""
        with patch("fluxmq.adapter.mqtt.Client", return_value=mock_mqtt_client):
            mqtt = MQTT(host="localhost", port=1883)
            mqtt._client = mock_mqtt_client
            mqtt._connected = False
            
            with pytest.raises(ConnectionError):
                await mqtt.unsubscribe("test/topic")
    
    @pytest.mark.asyncio
    async def test_request(self, mock_mqtt_client):
        """Test sending a request to MQTT and receiving a response."""
        with patch("fluxmq.adapter.mqtt.Client", return_value=mock_mqtt_client):
            mqtt = MQTT(host="localhost", port=1883)
            mqtt._client = mock_mqtt_client
            mqtt._connected = True
            
            # Mock uuid.uuid4 to return a predictable value
            with patch("uuid.uuid4", return_value=uuid.UUID("12345678-1234-5678-1234-567812345678")):
                # Mock the response future
                future = asyncio.Future()
                future.set_result(Message(
                    topic="response/topic",
                    data={"response": "test_response"},
                    headers={"correlation-id": "12345678-1234-5678-1234-567812345678"}
                ))
                mqtt._response_futures["12345678-1234-5678-1234-567812345678"] = future
                
                response = await mqtt.request(
                    topic="test/request",
                    data={"request": "test_request"},
                    timeout=2.0,
                    headers={"content-type": "application/json"}
                )
                
                assert response is not None
                assert response.data == {"response": "test_response"}
                # Check that the request was published with the correct correlation ID
                mock_mqtt_client.publish.assert_called_once()
                call_args = mock_mqtt_client.publish.call_args[0]
                assert call_args[0] == "test/request"  # topic
                payload = json.loads(call_args[1].decode())
                assert payload["request"] == "test_request"
                # Check that a temporary subscription was created for the response
                mock_mqtt_client.subscribe.assert_called_once()
                mock_mqtt_client.message_callback_add.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_request_timeout(self, mock_mqtt_client):
        """Test that a request times out if no response is received."""
        with patch("fluxmq.adapter.mqtt.Client", return_value=mock_mqtt_client):
            mqtt = MQTT(host="localhost", port=1883)
            mqtt._client = mock_mqtt_client
            mqtt._connected = True
            
            # Mock uuid.uuid4 to return a predictable value
            with patch("uuid.uuid4", return_value=uuid.UUID("12345678-1234-5678-1234-567812345678")):
                # Create a future that will time out
                future = asyncio.Future()
                mqtt._response_futures["12345678-1234-5678-1234-567812345678"] = future
                
                # Mock asyncio.wait_for to raise TimeoutError
                with patch("asyncio.wait_for", side_effect=asyncio.TimeoutError):
                    with pytest.raises(TimeoutError):
                        await mqtt.request(
                            topic="test/request",
                            data={"request": "test_request"},
                            timeout=0.1
                        )
    
    @pytest.mark.asyncio
    async def test_respond(self, mock_mqtt_client):
        """Test responding to an MQTT request."""
        with patch("fluxmq.adapter.mqtt.Client", return_value=mock_mqtt_client):
            mqtt = MQTT(host="localhost", port=1883)
            mqtt._client = mock_mqtt_client
            mqtt._connected = True
            
            # Create a request message with a reply topic
            request_msg = Message(
                topic="test/request",
                data={"request": "test_request"},
                reply="test/reply",
                headers={"correlation-id": "12345678-1234-5678-1234-567812345678"}
            )
            
            result = await mqtt.respond(
                request_message=request_msg,
                data={"response": "test_response"},
                headers={"content-type": "application/json"}
            )
            
            assert result is True
            mock_mqtt_client.publish.assert_called_once()
            # Check that the response was sent to the correct reply topic
            call_args = mock_mqtt_client.publish.call_args[0]
            assert call_args[0] == "test/reply"  # topic
            payload = json.loads(call_args[1].decode())
            assert payload["response"] == "test_response"
    
    @pytest.mark.asyncio
    async def test_respond_no_reply_topic(self, mock_mqtt_client):
        """Test responding to a request with no reply topic raises an error."""
        with patch("fluxmq.adapter.mqtt.Client", return_value=mock_mqtt_client):
            mqtt = MQTT(host="localhost", port=1883)
            mqtt._client = mock_mqtt_client
            mqtt._connected = True
            
            # Create a request message without a reply topic
            request_msg = Message(
                topic="test/request",
                data={"request": "test_request"}
            )
            
            with pytest.raises(ValueError):
                await mqtt.respond(
                    request_message=request_msg,
                    data={"response": "test_response"}
                )
    
    @pytest.mark.asyncio
    async def test_respond_not_connected(self, mock_mqtt_client):
        """Test responding when not connected raises an error."""
        with patch("fluxmq.adapter.mqtt.Client", return_value=mock_mqtt_client):
            mqtt = MQTT(host="localhost", port=1883)
            mqtt._client = mock_mqtt_client
            mqtt._connected = False
            
            request_msg = Message(
                topic="test/request",
                data={"request": "test_request"},
                reply="test/reply"
            )
            
            with pytest.raises(ConnectionError):
                await mqtt.respond(
                    request_message=request_msg,
                    data={"response": "test_response"}
                )


class TestMQTTTopic:
    """Tests for the MQTTTopic class."""
    
    def test_topic_conversion(self):
        """Test that MQTTTopic correctly uses slash notation."""
        topic = MQTTTopic(prefix="test")
        
        # Test status topic
        status_topic = topic.status("service1")
        assert status_topic == "test/status/service1"
        
        # Test service state topics
        set_state_topic = topic.set_service_state("service1")
        assert set_state_topic == "test/state/service1"
        
        get_state_topic = topic.get_service_state("service1")
        assert get_state_topic == "test/state/service1"
        
        # Test common data topics
        set_data_topic = topic.set_common_data("key1")
        assert set_data_topic == "test/common/key1"
        
        get_data_topic = topic.get_common_data("key1")
        assert get_data_topic == "test/common/key1"
        
        # Test command topics
        service_cmd_topic = topic.service_command("service1", "start")
        assert service_cmd_topic == "test/command/service1/start"
        
        node_cmd_topic = topic.node_command("node1", "restart")
        assert node_cmd_topic == "test/node/node1/restart"
        
        # Test node status topic
        node_status_topic = topic.node_status("node1")
        assert node_status_topic == "test/node/node1/status"


class TestMQTTStatus:
    """Tests for the MQTTStatus class."""
    
    def test_status_methods(self):
        """Test the status methods of MQTTStatus."""
        status = MQTTStatus()
        
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