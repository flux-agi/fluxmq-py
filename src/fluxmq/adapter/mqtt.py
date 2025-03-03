import asyncio
import json
import traceback
from asyncio import Queue
from logging import Logger, getLogger
from typing import Dict, List, Optional, Callable, Union, Any, Awaitable

import paho.mqtt.client as mqtt

from fluxmq.message import Message
from fluxmq.status import Status, StandardStatus
from fluxmq.topic import Topic, StandardTopic
from fluxmq.transport import Transport, SyncTransport


class MQTT(Transport):
    """
    MQTT implementation of the Transport interface.
    
    This class provides an asynchronous interface to the MQTT messaging system.
    """
    
    client: mqtt.Client
    logger: Logger
    host: str
    port: int
    username: Optional[str]
    password: Optional[str]
    client_id: str
    subscriptions: Dict[str, List[Callable[[Message], Awaitable[None]]]]
    connected: bool
    
    def __init__(self, 
                 host: str = "localhost", 
                 port: int = 1883, 
                 username: Optional[str] = None, 
                 password: Optional[str] = None,
                 client_id: Optional[str] = None,
                 logger: Optional[Logger] = None):
        """
        Initialize a new MQTT transport.
        
        Args:
            host: MQTT broker hostname
            port: MQTT broker port
            username: Optional username for authentication
            password: Optional password for authentication
            client_id: Optional client ID
            logger: Optional logger instance
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.client_id = client_id or ""
        self.subscriptions = {}
        self.connected = False
        
        if logger is None:
            self.logger = getLogger("fluxmq.mqtt")
        else:
            self.logger = logger
            
        # Initialize the MQTT client
        self.client = mqtt.Client(client_id=self.client_id)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        
        # Set up authentication if provided
        if self.username and self.password:
            self.client.username_pw_set(self.username, self.password)
    
    def _on_connect(self, client, userdata, flags, rc):
        """
        Callback for when the client connects to the broker.
        """
        if rc == 0:
            self.connected = True
            self.logger.info(f"Connected to MQTT broker at {self.host}:{self.port}")
            
            # Resubscribe to all topics
            for topic in self.subscriptions.keys():
                self.client.subscribe(topic)
                self.logger.debug(f"Resubscribed to topic: {topic}")
        else:
            self.connected = False
            self.logger.error(f"Failed to connect to MQTT broker, return code: {rc}")
    
    def _on_disconnect(self, client, userdata, rc):
        """
        Callback for when the client disconnects from the broker.
        """
        self.connected = False
        if rc != 0:
            self.logger.warning(f"Unexpected disconnection from MQTT broker, return code: {rc}")
        else:
            self.logger.info("Disconnected from MQTT broker")
    
    async def _on_message(self, client, userdata, msg):
        """
        Callback for when a message is received from the broker.
        """
        try:
            topic = msg.topic
            if topic in self.subscriptions:
                message = Message(
                    data=msg.payload,
                    reply=None,  # MQTT doesn't have a built-in reply concept
                    headers={}
                )
                
                # Call all handlers for this topic
                for handler in self.subscriptions[topic]:
                    try:
                        await handler(message)
                    except Exception as e:
                        self.logger.error(f"Error in message handler for topic {topic}: {str(e)}")
                        self.logger.debug(f"Exception details: {traceback.format_exc()}")
        except Exception as e:
            self.logger.error(f"Error processing MQTT message: {str(e)}")
            self.logger.debug(f"Exception details: {traceback.format_exc()}")

    async def connect(self) -> None:
        """
        Connect to the MQTT broker.
        
        Raises:
            ConnectionError: If connection to the MQTT broker fails
        """
        try:
            self.client.connect(self.host, self.port)
            self.client.loop_start()
            
            # Wait for connection to be established
            for _ in range(10):  # Try for up to 5 seconds
                if self.connected:
                    break
                await asyncio.sleep(0.5)
                
            if not self.connected:
                raise ConnectionError(f"Failed to connect to MQTT broker at {self.host}:{self.port}")
                
            self.logger.debug(f"Connected to MQTT broker at {self.host}:{self.port}")
        except Exception as e:
            self.logger.error(f"Failed to connect to MQTT broker: {str(e)}")
            self.logger.debug(f"Exception details: {traceback.format_exc()}")
            raise ConnectionError(f"Failed to connect to MQTT broker: {str(e)}") from e

    async def close(self) -> None:
        """
        Close the connection to the MQTT broker.
        """
        try:
            self.client.loop_stop()
            self.client.disconnect()
            self.connected = False
            self.logger.debug("Closed connection to MQTT broker")
        except Exception as e:
            self.logger.error(f"Error closing MQTT connection: {str(e)}")
            self.logger.debug(f"Exception details: {traceback.format_exc()}")
            raise

    async def publish(self, topic: str, payload: Union[bytes, str]) -> None:
        """
        Publish a message to a topic.
        
        Args:
            topic: The topic to publish to
            payload: The message payload to publish
            
        Raises:
            ConnectionError: If not connected to the MQTT broker
            ValueError: If the topic or payload is invalid
        """
        if not self.connected:
            raise ConnectionError("Not connected to MQTT broker")
            
        try:
            if not isinstance(payload, bytes):
                if isinstance(payload, str):
                    payload = payload.encode('utf-8')
                else:
                    payload = json.dumps(payload).encode('utf-8')
                    
            result = self.client.publish(topic, payload)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                raise ValueError(f"Failed to publish message to topic {topic}, error code: {result.rc}")
                
            self.logger.debug(f"Published message to topic: {topic}")
        except Exception as e:
            self.logger.error(f"Failed to publish message to topic {topic}: {str(e)}")
            self.logger.debug(f"Exception details: {traceback.format_exc()}")
            raise

    async def subscribe(self, topic: str, handler: Callable[[Message], Awaitable[None]]) -> str:
        """
        Subscribe to a topic with a message handler.
        
        Args:
            topic: The topic to subscribe to
            handler: Async callback function that will be called with each message
            
        Returns:
            The topic string as a subscription identifier
            
        Raises:
            ConnectionError: If not connected to the MQTT broker
            ValueError: If the topic is invalid
        """
        if not self.connected:
            raise ConnectionError("Not connected to MQTT broker")
            
        try:
            # Add the handler to the subscriptions dictionary
            if topic not in self.subscriptions:
                self.subscriptions[topic] = []
                
                # Subscribe to the topic
                result = self.client.subscribe(topic)
                if result[0] != mqtt.MQTT_ERR_SUCCESS:
                    raise ValueError(f"Failed to subscribe to topic {topic}, error code: {result[0]}")
                
            self.subscriptions[topic].append(handler)
            self.logger.debug(f"Subscribed to topic: {topic}")
            return topic
        except Exception as e:
            self.logger.error(f"Failed to subscribe to topic {topic}: {str(e)}")
            self.logger.debug(f"Exception details: {traceback.format_exc()}")
            raise

    async def unsubscribe(self, topic: str) -> None:
        """
        Unsubscribe from a topic.
        
        Args:
            topic: The topic to unsubscribe from
            
        Raises:
            ConnectionError: If not connected to the MQTT broker
            ValueError: If the topic is invalid or not subscribed
        """
        if not self.connected:
            raise ConnectionError("Not connected to MQTT broker")
            
        if topic in self.subscriptions:
            try:
                result = self.client.unsubscribe(topic)
                if result[0] != mqtt.MQTT_ERR_SUCCESS:
                    raise ValueError(f"Failed to unsubscribe from topic {topic}, error code: {result[0]}")
                    
                del self.subscriptions[topic]
                self.logger.debug(f"Unsubscribed from topic: {topic}")
            except Exception as e:
                self.logger.error(f"Failed to unsubscribe from topic {topic}: {str(e)}")
                self.logger.debug(f"Exception details: {traceback.format_exc()}")
                raise
        else:
            self.logger.warning(f"Attempted to unsubscribe from topic {topic} that was not subscribed")

    async def request(self, topic: str, payload: Union[bytes, str]) -> Message:
        """
        Send a request and wait for a response.
        
        This implements a request-reply pattern over MQTT using a temporary
        response topic.
        
        Args:
            topic: The topic to send the request to
            payload: The request payload
            
        Returns:
            The response message
            
        Raises:
            ConnectionError: If not connected to the MQTT broker
            TimeoutError: If the request times out
            ValueError: If the topic or payload is invalid
        """
        if not self.connected:
            raise ConnectionError("Not connected to MQTT broker")
            
        try:
            # Create a unique response topic
            response_topic = f"{topic}/response/{id(self)}-{id(payload)}"
            response_queue = asyncio.Queue(maxsize=1)
            
            # Define a handler for the response
            async def response_handler(message: Message) -> None:
                await response_queue.put(message)
            
            # Subscribe to the response topic
            await self.subscribe(response_topic, response_handler)
            
            try:
                # Add the response topic to the payload if it's a dict
                if not isinstance(payload, (bytes, str)):
                    payload = {**payload, "response_topic": response_topic}
                
                # Publish the request
                await self.publish(topic, payload)
                
                # Wait for the response with a timeout
                try:
                    response = await asyncio.wait_for(response_queue.get(), timeout=10.0)
                    return response
                except asyncio.TimeoutError:
                    raise TimeoutError(f"Request to topic {topic} timed out")
            finally:
                # Unsubscribe from the response topic
                await self.unsubscribe(response_topic)
        except Exception as e:
            if not isinstance(e, TimeoutError):
                self.logger.error(f"Failed to send request to topic {topic}: {str(e)}")
                self.logger.debug(f"Exception details: {traceback.format_exc()}")
            raise

    async def respond(self, message: Message, response: Union[bytes, str]) -> None:
        """
        Respond to a request message.
        
        Args:
            message: The request message to respond to
            response: The response data
            
        Raises:
            ConnectionError: If not connected to the MQTT broker
            ValueError: If the message or response is invalid
        """
        if not self.connected:
            raise ConnectionError("Not connected to MQTT broker")
            
        # Extract the response topic from the message
        response_topic = None
        if isinstance(message.data, bytes):
            try:
                data = json.loads(message.data.decode('utf-8'))
                if isinstance(data, dict) and "response_topic" in data:
                    response_topic = data["response_topic"]
            except:
                pass
        elif isinstance(message.data, dict) and "response_topic" in message.data:
            response_topic = message.data["response_topic"]
            
        if not response_topic:
            raise ValueError("Cannot respond to a message without a response topic")
            
        # Publish the response to the response topic
        await self.publish(response_topic, response)


class MQTTTopic(StandardTopic):
    """
    MQTT implementation of the Topic interface.
    
    This class provides topic naming conventions for MQTT.
    """
    
    def __init__(self, prefix: Optional[str] = None):
        """
        Initialize a new MQTTTopic.
        
        Args:
            prefix: Optional prefix to prepend to all topics
        """
        super().__init__(prefix)


class MQTTStatus(StandardStatus):
    """
    MQTT implementation of the Status interface.
    
    This class provides status values for MQTT.
    """
    pass
