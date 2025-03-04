import asyncio
import threading
import json
import traceback
import nats
import concurrent.futures

from asyncio import Queue
from logging import Logger, getLogger
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from typing import Callable, Dict, List, TypeVar, Generic, Optional, Union, Any, Awaitable

from fluxmq.message import Message
from fluxmq.status import Status, StandardStatus
from fluxmq.topic import Topic, StandardTopic
from fluxmq.transport import Transport, SyncTransport

MessageType = TypeVar('MessageType', bound=Message)

class TypedQueue(Queue, Generic[MessageType]):
    """
    A typed queue for handling specific message types.
    
    This extends the standard asyncio Queue with generic type information.
    """
    pass

class Nats(Transport):
    """
    NATS implementation of the Transport interface.
    
    This class provides an asynchronous interface to the NATS messaging system.
    """
    
    connection: Optional[NATS]
    logger: Logger
    servers: List[str]
    subscriptions: Dict[str, Subscription]

    def __init__(self, servers: List[str], logger: Optional[Logger] = None):
        """
        Initialize a new NATS transport.
        
        Args:
            servers: List of NATS server URLs
            logger: Optional logger instance
        """
        self.servers = servers
        self.subscriptions = {}
        self.connection = None
        
        if logger is None:
            self.logger = getLogger("fluxmq.nats")
        else:
            self.logger = logger

    async def connect(self) -> None:
        """
        Connect to the NATS server.
        
        Raises:
            ConnectionError: If connection to the NATS server fails
        """
        try:
            # Add more connection options for reliability
            self.connection = await nats.connect(
                servers=self.servers,
                reconnect_time_wait=2,
                max_reconnect_attempts=10,
                connect_timeout=10
            )
            self.logger.debug(f"Connected to NATS servers: {self.servers}")
        except Exception as e:
            self.logger.error(f"Failed to connect to NATS servers: {str(e)}")
            self.logger.debug(f"Exception details: {traceback.format_exc()}")
            raise ConnectionError(f"Failed to connect to NATS servers: {str(e)}") from e

    async def publish(self, topic: str, payload: Union[bytes, str]) -> None:
        """
        Publish a message to a topic.
        
        Args:
            topic: The topic to publish to
            payload: The message payload to publish
            
        Raises:
            ConnectionError: If not connected to the NATS server
            ValueError: If the topic or payload is invalid
        """
        if self.connection is None:
            raise ConnectionError("Not connected to NATS server")
            
        try:
            if not isinstance(payload, bytes):
                if isinstance(payload, str):
                    payload = payload.encode('utf-8')
                else:
                    payload = json.dumps(payload).encode('utf-8')
                    
            await self.connection.publish(topic, payload)
            self.logger.debug(f"Published message to topic: {topic}")
        except Exception as e:
            self.logger.error(f"Failed to publish message to topic {topic}: {str(e)}")
            self.logger.debug(f"Exception details: {traceback.format_exc()}")
            raise

    async def subscribe(self, topic: str, handler: Callable[[Message], Awaitable[None]]) -> Subscription:
        """
        Subscribe to a topic with a message handler.
        
        Args:
            topic: The topic to subscribe to
            handler: Async callback function that will be called with each message
            
        Returns:
            A subscription object that can be used to unsubscribe
            
        Raises:
            ConnectionError: If not connected to the NATS server
            ValueError: If the topic is invalid
        """
        if self.connection is None:
            raise ConnectionError("Not connected to NATS server")
            
        try:
            async def message_handler(raw: Msg) -> None:
                try:
                    message = Message(
                        data=raw.data,
                        reply=raw.reply,
                        headers=raw.header
                    )
                    await handler(message)
                except Exception as e:
                    self.logger.error(f"Error in message handler for topic {topic}: {str(e)}")
                    self.logger.debug(f"Exception details: {traceback.format_exc()}")
            
            subscription = await self.connection.subscribe(topic, cb=message_handler)
            self.subscriptions[topic] = subscription
            self.logger.debug(f"Subscribed to topic: {topic}")
            return subscription
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
            ConnectionError: If not connected to the NATS server
            ValueError: If the topic is invalid or not subscribed
        """
        if topic in self.subscriptions:
            try:
                await self.subscriptions[topic].unsubscribe()
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
        
        Args:
            topic: The topic to send the request to
            payload: The request payload
            
        Returns:
            The response message
            
        Raises:
            ConnectionError: If not connected to the NATS server
            TimeoutError: If the request times out
            ValueError: If the topic or payload is invalid
        """
        if self.connection is None:
            raise ConnectionError("Not connected to NATS server")
            
        try:
            if not isinstance(payload, bytes):
                if isinstance(payload, str):
                    payload = payload.encode('utf-8')
                else:
                    payload = json.dumps(payload).encode('utf-8')
                    
            response = await self.connection.request(topic, payload)
            self.logger.debug(f"Received response for request to topic: {topic}")
            
            return Message(
                data=response.data,
                reply=response.reply,
                headers=response.header
            )
        except nats.aio.errors.ErrTimeout as e:
            self.logger.error(f"Request to topic {topic} timed out")
            raise TimeoutError(f"Request to topic {topic} timed out") from e
        except Exception as e:
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
            ConnectionError: If not connected to the NATS server
            ValueError: If the message or response is invalid
        """
        if self.connection is None:
            raise ConnectionError("Not connected to NATS server")
            
        if not message.reply:
            raise ValueError("Cannot respond to a message without a reply subject")
            
        try:
            if not isinstance(response, bytes):
                if isinstance(response, str):
                    response = response.encode('utf-8')
                else:
                    response = json.dumps(response).encode('utf-8')
                    
            await self.connection.publish(message.reply, response)
            self.logger.debug(f"Sent response to reply subject: {message.reply}")
        except Exception as e:
            self.logger.error(f"Failed to send response: {str(e)}")
            self.logger.debug(f"Exception details: {traceback.format_exc()}")
            raise

    async def close(self) -> None:
        """
        Close the connection to the NATS server.
        """
        if self.connection:
            try:
                await self.connection.drain()
                await self.connection.close()
                self.connection = None
                self.logger.debug("Closed connection to NATS server")
            except Exception as e:
                self.logger.error(f"Error closing NATS connection: {str(e)}")
                self.logger.debug(f"Exception details: {traceback.format_exc()}")
                raise


class SyncNats(SyncTransport):
    """
    Synchronous NATS implementation of the SyncTransport interface.
    
    This class provides a synchronous interface to the NATS messaging system,
    using a background thread to run the asyncio event loop.
    """
    
    def __init__(self, servers: List[str], logger: Optional[Logger] = None):
        """
        Initialize a new synchronous NATS transport.
        
        Args:
            servers: List of NATS server URLs
            logger: Optional logger instance
        """
        self._servers = servers
        self._nc = None
        self._subscriptions = {}
        self._lock = threading.RLock()
        self._loop = None
        self._thread = None
        
        if logger is None:
            self._logger = getLogger("fluxmq.nats.sync")
        else:
            self._logger = logger

    def connect(self) -> bool:
        """
        Connect to the NATS server.
        
        Returns:
            True if the connection was successful
            
        Raises:
            ConnectionError: If connection to the NATS server fails
        """
        with self._lock:
            if self._nc is not None:
                self._logger.warning("Already connected to NATS server")
                return True
                
            try:
                # Create a new event loop
                self._loop = asyncio.new_event_loop()
                
                # Create and start the background thread
                self._thread = threading.Thread(
                    target=self._run_event_loop,
                    daemon=True
                )
                self._thread.start()
                
                # Run the connect coroutine in the event loop
                future = asyncio.run_coroutine_threadsafe(
                    self._connect(),
                    self._loop
                )
                
                # Wait for the connection to complete
                future.result(timeout=10)
                
                self._logger.debug(f"Connected to NATS servers: {self._servers}")
                return True
            except Exception as e:
                self._logger.error(f"Failed to connect to NATS servers: {str(e)}")
                self._logger.debug(f"Exception details: {traceback.format_exc()}")
                
                # Clean up resources
                if self._thread is not None and self._thread.is_alive():
                    if self._loop is not None:
                        asyncio.run_coroutine_threadsafe(
                            self._close(),
                            self._loop
                        )
                
                raise ConnectionError(f"Failed to connect to NATS servers: {str(e)}") from e

    async def _connect(self) -> None:
        """Internal coroutine to connect to the NATS server."""
        self._nc = await nats.connect(servers=self._servers)

    def _run_event_loop(self) -> None:
        """Run the asyncio event loop in a background thread."""
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()
        
        # Clean up when the loop stops
        pending = asyncio.all_tasks(self._loop)
        for task in pending:
            task.cancel()
            
        self._loop.run_until_complete(self._loop.shutdown_asyncgens())
        self._loop.close()

    def publish(self, topic: str, data: Any, headers: Optional[Dict[str, str]] = None) -> bool:
        """
        Publish a message to a topic.
        
        Args:
            topic: The topic to publish to
            data: The message data to publish
            headers: Optional headers to include with the message
            
        Returns:
            True if the message was published successfully
            
        Raises:
            ConnectionError: If not connected to the NATS server
            ValueError: If the topic or data is invalid
        """
        with self._lock:
            if self._nc is None or self._loop is None:
                raise ConnectionError("Not connected to NATS server")
                
            try:
                # Convert data to bytes
                payload = data
                if not isinstance(payload, bytes):
                    if isinstance(payload, str):
                        payload = payload.encode('utf-8')
                    else:
                        payload = json.dumps(payload).encode('utf-8')
                
                # Run the publish coroutine in the event loop
                future = asyncio.run_coroutine_threadsafe(
                    self._nc.publish(topic, payload, headers=headers),
                    self._loop
                )
                
                # Wait for the publish to complete
                future.result(timeout=10)
                
                self._logger.debug(f"Published message to topic: {topic}")
                return True
            except Exception as e:
                self._logger.error(f"Failed to publish message to topic {topic}: {str(e)}")
                self._logger.debug(f"Exception details: {traceback.format_exc()}")
                raise

    def subscribe(self, topic: str, handler: Callable[[Message], None]) -> str:
        """
        Subscribe to a topic with a message handler.
        
        Args:
            topic: The topic to subscribe to
            handler: Callback function that will be called with each message
            
        Returns:
            A subscription ID that can be used to unsubscribe
            
        Raises:
            ConnectionError: If not connected to the NATS server
            ValueError: If the topic is invalid
        """
        with self._lock:
            if self._nc is None or self._loop is None:
                raise ConnectionError("Not connected to NATS server")
                
            try:
                # Create an async message handler that calls the sync handler
                async def message_handler(raw: Msg) -> None:
                    try:
                        message = Message(
                            topic=raw.subject,
                            data=raw.data,
                            reply=raw.reply,
                            headers=raw.header
                        )
                        # Call the handler in a thread to avoid blocking the event loop
                        self._loop.run_in_executor(None, handler, message)
                    except Exception as e:
                        self._logger.error(f"Error in message handler for topic {topic}: {str(e)}")
                        self._logger.debug(f"Exception details: {traceback.format_exc()}")
                
                # Run the subscribe coroutine in the event loop
                future = asyncio.run_coroutine_threadsafe(
                    self._nc.subscribe(topic, cb=message_handler),
                    self._loop
                )
                
                # Wait for the subscription to complete
                subscription = future.result(timeout=10)
                
                # Generate a unique subscription ID
                subscription_id = f"sub_{id(subscription)}"
                self._subscriptions[subscription_id] = subscription
                
                self._logger.debug(f"Subscribed to topic: {topic}")
                return subscription_id
            except Exception as e:
                self._logger.error(f"Failed to subscribe to topic {topic}: {str(e)}")
                self._logger.debug(f"Exception details: {traceback.format_exc()}")
                raise

    def unsubscribe(self, subscription_id: str) -> bool:
        """
        Unsubscribe from a topic.
        
        Args:
            subscription_id: The subscription ID returned from subscribe
            
        Returns:
            True if the unsubscribe was successful
            
        Raises:
            ConnectionError: If not connected to the NATS server
            ValueError: If the subscription ID is invalid
        """
        with self._lock:
            if self._nc is None or self._loop is None:
                raise ConnectionError("Not connected to NATS server")
                
            if subscription_id not in self._subscriptions:
                self._logger.warning(f"Attempted to unsubscribe from unknown subscription: {subscription_id}")
                return False
                
            try:
                subscription = self._subscriptions[subscription_id]
                
                # Run the unsubscribe coroutine in the event loop
                future = asyncio.run_coroutine_threadsafe(
                    subscription.unsubscribe(),
                    self._loop
                )
                
                # Wait for the unsubscribe to complete
                future.result(timeout=10)
                
                # Remove the subscription from our tracking
                del self._subscriptions[subscription_id]
                
                self._logger.debug(f"Unsubscribed from subscription: {subscription_id}")
                return True
            except Exception as e:
                self._logger.error(f"Failed to unsubscribe from subscription {subscription_id}: {str(e)}")
                self._logger.debug(f"Exception details: {traceback.format_exc()}")
                raise

    def close(self) -> bool:
        """
        Close the connection to the NATS server.
        
        Returns:
            True if the connection was closed successfully
        """
        with self._lock:
            if self._nc is None or self._loop is None:
                self._logger.warning("Not connected to NATS server")
                return True
                
            try:
                # Run the close coroutine in the event loop
                future = asyncio.run_coroutine_threadsafe(
                    self._close(),
                    self._loop
                )
                
                # Wait for the close to complete
                future.result(timeout=10)
                
                # Stop the event loop
                self._loop.call_soon_threadsafe(self._loop.stop)
                
                # Wait for the thread to finish
                if self._thread is not None and self._thread.is_alive():
                    self._thread.join(timeout=5)
                
                # Reset state
                self._nc = None
                self._loop = None
                self._thread = None
                self._subscriptions = {}
                
                self._logger.debug("Closed connection to NATS server")
                return True
            except Exception as e:
                self._logger.error(f"Error closing NATS connection: {str(e)}")
                self._logger.debug(f"Exception details: {traceback.format_exc()}")
                return False

    async def _close(self) -> None:
        """Internal coroutine to close the NATS connection."""
        if self._nc is not None:
            await self._nc.drain()
            await self._nc.close()

    def request(self, topic: str, data: Any, timeout: float = 5.0, headers: Optional[Dict[str, str]] = None) -> Message:
        """
        Send a request and wait for a response.
        
        Args:
            topic: The topic to send the request to
            data: The request data
            timeout: The timeout in seconds
            headers: Optional headers to include with the request
            
        Returns:
            The response message
            
        Raises:
            ConnectionError: If not connected to the NATS server
            TimeoutError: If the request times out
            ValueError: If the topic or data is invalid
        """
        with self._lock:
            if self._nc is None or self._loop is None:
                raise ConnectionError("Not connected to NATS server")
                
            try:
                # Convert data to bytes
                payload = data
                if not isinstance(payload, bytes):
                    if isinstance(payload, str):
                        payload = payload.encode('utf-8')
                    else:
                        payload = json.dumps(payload).encode('utf-8')
                
                # Run the request coroutine in the event loop
                future = asyncio.run_coroutine_threadsafe(
                    self._nc.request(topic, payload, timeout=timeout, headers=headers),
                    self._loop
                )
                
                # Wait for the response
                try:
                    response = future.result(timeout=timeout + 1)  # Add 1 second buffer
                    
                    self._logger.debug(f"Received response for request to topic: {topic}")
                    
                    return Message(
                        topic=response.subject,
                        data=response.data,
                        reply=response.reply,
                        headers=response.header
                    )
                except concurrent.futures.TimeoutError:
                    self._logger.error(f"Request to topic {topic} timed out")
                    raise TimeoutError(f"Request to topic {topic} timed out")
            except Exception as e:
                if isinstance(e, TimeoutError):
                    raise
                self._logger.error(f"Failed to send request to topic {topic}: {str(e)}")
                self._logger.debug(f"Exception details: {traceback.format_exc()}")
                raise

    def respond(self, request_message: Message, data: Any, headers: Optional[Dict[str, str]] = None) -> bool:
        """
        Respond to a request message.
        
        Args:
            request_message: The request message to respond to
            data: The response data
            headers: Optional headers to include with the response
            
        Returns:
            True if the response was sent successfully
            
        Raises:
            ConnectionError: If not connected to the NATS server
            ValueError: If the message has no reply topic or the data is invalid
        """
        with self._lock:
            if self._nc is None or self._loop is None:
                raise ConnectionError("Not connected to NATS server")
                
            if not request_message.reply:
                raise ValueError("Cannot respond to a message without a reply topic")
                
            try:
                # Convert data to bytes
                payload = data
                if not isinstance(payload, bytes):
                    if isinstance(payload, str):
                        payload = payload.encode('utf-8')
                    else:
                        payload = json.dumps(payload).encode('utf-8')
                
                # Run the publish coroutine in the event loop
                future = asyncio.run_coroutine_threadsafe(
                    self._nc.publish(request_message.reply, payload, headers=headers),
                    self._loop
                )
                
                # Wait for the publish to complete
                future.result(timeout=10)
                
                self._logger.debug(f"Sent response to reply topic: {request_message.reply}")
                return True
            except Exception as e:
                self._logger.error(f"Failed to send response: {str(e)}")
                self._logger.debug(f"Exception details: {traceback.format_exc()}")
                raise


class NatsTopic(StandardTopic):
    """
    NATS implementation of the Topic interface.
    
    This class provides topic naming conventions for NATS.
    """
    
    def __init__(self, prefix: Optional[str] = None):
        """
        Initialize a new NatsTopic.
        
        Args:
            prefix: Optional prefix to prepend to all topics
        """
        super().__init__(prefix)


class NatsStatus(StandardStatus):
    """
    NATS implementation of the Status interface.
    
    This class provides status values for NATS.
    """
    pass
