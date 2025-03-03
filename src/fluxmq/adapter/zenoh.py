"""
Zenoh adapter for FluxMQ.

This module provides a Zenoh implementation of the Transport interface.
"""
import asyncio
import concurrent.futures
import json
import threading
import traceback
import uuid
from logging import Logger, getLogger
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union

try:
    import zenoh
    from zenoh.session import Session
    from zenoh.queryable import Queryable
    from zenoh.subscriber import Subscriber
except ImportError:
    raise ImportError(
        "Zenoh is not installed. Please install it with 'pip install zenoh'"
    )

from fluxmq.message import Message
from fluxmq.status import Status, StandardStatus
from fluxmq.topic import Topic, StandardTopic
from fluxmq.transport import Transport, SyncTransport 

class Zenoh(Transport):
    """
    Zenoh implementation of the Transport interface.
    
    This class provides an asynchronous interface to the Zenoh messaging system.
    """
    
    def __init__(self, 
                 config: Optional[Dict[str, Any]] = None, 
                 logger: Optional[Logger] = None):
        """
        Initialize a new Zenoh transport.
        
        Args:
            config: Optional Zenoh configuration dictionary
            logger: Optional logger instance
        """
        self._config = config or {}
        self._session = None
        self._subscriptions = {}
        self._queryables = {}
        self._response_futures = {}
        self._connected = False
        
        if logger is None:
            self._logger = getLogger("fluxmq.zenoh")
        else:
            self._logger = logger
    
    async def connect(self) -> bool:
        """
        Connect to the Zenoh network.
        
        Returns:
            True if the connection was successful
            
        Raises:
            ConnectionError: If connection to the Zenoh network fails
        """
        if self._connected:
            self._logger.warning("Already connected to Zenoh network")
            return True
            
        try:
            self._session = await zenoh.open(self._config)
            self._connected = True
            self._logger.debug("Connected to Zenoh network")
            return True
        except Exception as e:
            self._logger.error(f"Failed to connect to Zenoh network: {str(e)}")
            self._logger.debug(f"Exception details: {traceback.format_exc()}")
            raise ConnectionError(f"Failed to connect to Zenoh network: {str(e)}") from e
    
    async def close(self) -> bool:
        """
        Close the connection to the Zenoh network.
        
        Returns:
            True if the connection was closed successfully
        """
        if not self._connected or self._session is None:
            self._logger.warning("Not connected to Zenoh network")
            return True
            
        try:
            # Unsubscribe from all topics
            for sub_id, subscription in list(self._subscriptions.items()):
                await subscription.undeclare()
                del self._subscriptions[sub_id]
            
            # Undeclare all queryables
            for query_id, queryable in list(self._queryables.items()):
                await queryable.undeclare()
                del self._queryables[query_id]
            
            # Close the session
            await self._session.close()
            self._session = None
            self._connected = False
            self._logger.debug("Closed connection to Zenoh network")
            return True
        except Exception as e:
            self._logger.error(f"Error closing Zenoh connection: {str(e)}")
            self._logger.debug(f"Exception details: {traceback.format_exc()}")
            return False 
    
    async def publish(self, topic: str, data: Any, headers: Optional[Dict[str, str]] = None) -> bool:
        """
        Publish a message to a topic.
        
        Args:
            topic: The topic to publish to
            data: The message data to publish
            headers: Optional headers to include with the message
            
        Returns:
            True if the message was published successfully
            
        Raises:
            ConnectionError: If not connected to the Zenoh network
            ValueError: If the topic or data is invalid
        """
        if not self._connected or self._session is None:
            raise ConnectionError("Not connected to Zenoh network")
            
        try:
            # Convert data to the appropriate format
            payload = data
            if not isinstance(payload, bytes):
                if isinstance(payload, str):
                    payload = payload.encode('utf-8')
                else:
                    # Convert to JSON
                    payload = json.dumps(payload).encode('utf-8')
            
            # Create a value with headers if provided
            if headers:
                value = zenoh.Value(payload, encoding=zenoh.Encoding.APP_OCTET_STREAM)
                for key, val in headers.items():
                    value.put_attachment(key.encode(), val.encode())
            else:
                value = zenoh.Value(payload)
            
            # Publish the message
            await self._session.put(topic, value)
            self._logger.debug(f"Published message to topic: {topic}")
            return True
        except Exception as e:
            self._logger.error(f"Failed to publish message to topic {topic}: {str(e)}")
            self._logger.debug(f"Exception details: {traceback.format_exc()}")
            raise
    
    async def subscribe(self, topic: str, handler: Callable[[Message], Awaitable[None]]) -> str:
        """
        Subscribe to a topic with a message handler.
        
        Args:
            topic: The topic to subscribe to
            handler: Async callback function that will be called with each message
            
        Returns:
            A subscription ID that can be used to unsubscribe
            
        Raises:
            ConnectionError: If not connected to the Zenoh network
            ValueError: If the topic is invalid
        """
        if not self._connected or self._session is None:
            raise ConnectionError("Not connected to Zenoh network")
            
        try:
            # Create a callback function that converts Zenoh samples to FluxMQ messages
            async def sample_handler(sample):
                try:
                    # Extract data from the sample
                    data = sample.value.payload
                    
                    # Extract headers from attachments if any
                    headers = {}
                    for key, val in sample.value.attachment_items():
                        headers[key.decode()] = val.decode()
                    
                    # Create a FluxMQ message
                    message = Message(
                        topic=sample.key_expr.to_string(),
                        data=data,
                        headers=headers
                    )
                    
                    # Call the user's handler
                    await handler(message)
                except Exception as e:
                    self._logger.error(f"Error in message handler for topic {topic}: {str(e)}")
                    self._logger.debug(f"Exception details: {traceback.format_exc()}")
            
            # Subscribe to the topic
            subscriber = await self._session.declare_subscriber(topic, sample_handler)
            
            # Generate a unique subscription ID
            subscription_id = f"sub_{id(subscriber)}"
            self._subscriptions[subscription_id] = subscriber
            
            self._logger.debug(f"Subscribed to topic: {topic}")
            return subscription_id
        except Exception as e:
            self._logger.error(f"Failed to subscribe to topic {topic}: {str(e)}")
            self._logger.debug(f"Exception details: {traceback.format_exc()}")
            raise
    
    async def unsubscribe(self, subscription_id: str) -> bool:
        """
        Unsubscribe from a topic.
        
        Args:
            subscription_id: The subscription ID returned from subscribe
            
        Returns:
            True if the unsubscribe was successful
            
        Raises:
            ConnectionError: If not connected to the Zenoh network
            ValueError: If the subscription ID is invalid
        """
        if not self._connected or self._session is None:
            raise ConnectionError("Not connected to Zenoh network")
            
        if subscription_id not in self._subscriptions:
            self._logger.warning(f"Attempted to unsubscribe from unknown subscription: {subscription_id}")
            return False
            
        try:
            # Get the subscriber
            subscriber = self._subscriptions[subscription_id]
            
            # Undeclare the subscriber
            await subscriber.undeclare()
            
            # Remove from our tracking
            del self._subscriptions[subscription_id]
            
            self._logger.debug(f"Unsubscribed from subscription: {subscription_id}")
            return True
        except Exception as e:
            self._logger.error(f"Failed to unsubscribe from subscription {subscription_id}: {str(e)}")
            self._logger.debug(f"Exception details: {traceback.format_exc()}")
            raise 
    
    async def request(self, topic: str, data: Any, timeout: float = 5.0, headers: Optional[Dict[str, str]] = None) -> Message:
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
            ConnectionError: If not connected to the Zenoh network
            TimeoutError: If the request times out
            ValueError: If the topic or data is invalid
        """
        if not self._connected or self._session is None:
            raise ConnectionError("Not connected to Zenoh network")
            
        try:
            # Generate a unique correlation ID for this request
            correlation_id = str(uuid.uuid4())
            
            # Create a future to receive the response
            response_future = asyncio.Future()
            self._response_futures[correlation_id] = response_future
            
            # Create a temporary response topic
            response_topic = f"{topic}/response/{correlation_id}"
            
            # Create headers with correlation ID if not provided
            request_headers = headers.copy() if headers else {}
            request_headers["correlation-id"] = correlation_id
            request_headers["reply"] = response_topic
            
            # Subscribe to the response topic
            async def response_handler(message):
                # Check if this is the response we're waiting for
                msg_correlation_id = message.headers.get("correlation-id")
                if msg_correlation_id == correlation_id and not response_future.done():
                    response_future.set_result(message)
            
            sub_id = await self.subscribe(response_topic, response_handler)
            
            try:
                # Send the request
                await self.publish(topic, data, request_headers)
                
                # Wait for the response with timeout
                try:
                    response = await asyncio.wait_for(response_future, timeout)
                    return response
                except asyncio.TimeoutError:
                    self._logger.error(f"Request to topic {topic} timed out after {timeout} seconds")
                    raise TimeoutError(f"Request to topic {topic} timed out after {timeout} seconds")
            finally:
                # Clean up the temporary subscription
                await self.unsubscribe(sub_id)
                if correlation_id in self._response_futures:
                    del self._response_futures[correlation_id]
                
        except Exception as e:
            if isinstance(e, TimeoutError):
                raise
            self._logger.error(f"Failed to send request to topic {topic}: {str(e)}")
            self._logger.debug(f"Exception details: {traceback.format_exc()}")
            raise
    
    async def respond(self, request_message: Message, data: Any, headers: Optional[Dict[str, str]] = None) -> bool:
        """
        Respond to a request message.
        
        Args:
            request_message: The request message to respond to
            data: The response data
            headers: Optional headers to include with the response
            
        Returns:
            True if the response was sent successfully
            
        Raises:
            ConnectionError: If not connected to the Zenoh network
            ValueError: If the message has no reply topic or the data is invalid
        """
        if not self._connected or self._session is None:
            raise ConnectionError("Not connected to Zenoh network")
            
        # Get the reply topic from the request message
        reply_topic = request_message.headers.get("reply")
        if not reply_topic:
            raise ValueError("Cannot respond to a message without a reply topic")
            
        try:
            # Get the correlation ID from the request message
            correlation_id = request_message.headers.get("correlation-id")
            
            # Create headers with correlation ID if not provided
            response_headers = headers.copy() if headers else {}
            if correlation_id:
                response_headers["correlation-id"] = correlation_id
            
            # Send the response
            await self.publish(reply_topic, data, response_headers)
            self._logger.debug(f"Sent response to reply topic: {reply_topic}")
            return True
        except Exception as e:
            self._logger.error(f"Failed to send response: {str(e)}")
            self._logger.debug(f"Exception details: {traceback.format_exc()}")
            raise 

class SyncZenoh(SyncTransport):
    """
    Synchronous Zenoh implementation of the SyncTransport interface.
    
    This class provides a synchronous interface to the Zenoh messaging system,
    using a background thread to run the asyncio event loop.
    """
    
    def __init__(self, 
                 config: Optional[Dict[str, Any]] = None, 
                 logger: Optional[Logger] = None):
        """
        Initialize a new synchronous Zenoh transport.
        
        Args:
            config: Optional Zenoh configuration dictionary
            logger: Optional logger instance
        """
        self._config = config or {}
        self._session = None
        self._subscriptions = {}
        self._queryables = {}
        self._response_futures = {}
        self._connected = False
        self._lock = threading.RLock()
        self._loop = None
        self._thread = None
        
        if logger is None:
            self._logger = getLogger("fluxmq.zenoh.sync")
        else:
            self._logger = logger
    
    def connect(self) -> bool:
        """
        Connect to the Zenoh network.
        
        Returns:
            True if the connection was successful
            
        Raises:
            ConnectionError: If connection to the Zenoh network fails
        """
        with self._lock:
            if self._connected:
                self._logger.warning("Already connected to Zenoh network")
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
                
                self._connected = True
                self._logger.debug("Connected to Zenoh network")
                return True
            except Exception as e:
                self._logger.error(f"Failed to connect to Zenoh network: {str(e)}")
                self._logger.debug(f"Exception details: {traceback.format_exc()}")
                
                # Clean up resources
                if self._thread is not None and self._thread.is_alive():
                    if self._loop is not None:
                        asyncio.run_coroutine_threadsafe(
                            self._close(),
                            self._loop
                        )
                
                raise ConnectionError(f"Failed to connect to Zenoh network: {str(e)}") from e
    
    async def _connect(self) -> None:
        """Internal coroutine to connect to the Zenoh network."""
        self._session = await zenoh.open(self._config)
    
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
    
    def close(self) -> bool:
        """
        Close the connection to the Zenoh network.
        
        Returns:
            True if the connection was closed successfully
        """
        with self._lock:
            if not self._connected or self._session is None:
                self._logger.warning("Not connected to Zenoh network")
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
                self._session = None
                self._loop = None
                self._thread = None
                self._subscriptions = {}
                self._queryables = {}
                self._response_futures = {}
                self._connected = False
                
                self._logger.debug("Closed connection to Zenoh network")
                return True
            except Exception as e:
                self._logger.error(f"Error closing Zenoh connection: {str(e)}")
                self._logger.debug(f"Exception details: {traceback.format_exc()}")
                return False
    
    async def _close(self) -> None:
        """Internal coroutine to close the Zenoh connection."""
        if self._session is not None:
            # Unsubscribe from all topics
            for sub_id, subscription in list(self._subscriptions.items()):
                await subscription.undeclare()
            
            # Undeclare all queryables
            for query_id, queryable in list(self._queryables.items()):
                await queryable.undeclare()
            
            # Close the session
            await self._session.close()
    
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
            ConnectionError: If not connected to the Zenoh network
            ValueError: If the topic or data is invalid
        """
        with self._lock:
            if not self._connected or self._session is None or self._loop is None:
                raise ConnectionError("Not connected to Zenoh network")
                
            try:
                # Convert data to the appropriate format
                payload = data
                if not isinstance(payload, bytes):
                    if isinstance(payload, str):
                        payload = payload.encode('utf-8')
                    else:
                        # Convert to JSON
                        payload = json.dumps(payload).encode('utf-8')
                
                # Run the publish coroutine in the event loop
                future = asyncio.run_coroutine_threadsafe(
                    self._publish(topic, payload, headers),
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
    
    async def _publish(self, topic: str, payload: bytes, headers: Optional[Dict[str, str]] = None) -> None:
        """Internal coroutine to publish a message."""
        # Create a value with headers if provided
        if headers:
            value = zenoh.Value(payload, encoding=zenoh.Encoding.APP_OCTET_STREAM)
            for key, val in headers.items():
                value.put_attachment(key.encode(), val.encode())
        else:
            value = zenoh.Value(payload)
        
        # Publish the message
        await self._session.put(topic, value)
    
    def subscribe(self, topic: str, handler: Callable[[Message], None]) -> str:
        """
        Subscribe to a topic with a message handler.
        
        Args:
            topic: The topic to subscribe to
            handler: Callback function that will be called with each message
            
        Returns:
            A subscription ID that can be used to unsubscribe
            
        Raises:
            ConnectionError: If not connected to the Zenoh network
            ValueError: If the topic is invalid
        """
        with self._lock:
            if not self._connected or self._session is None or self._loop is None:
                raise ConnectionError("Not connected to Zenoh network")
                
            try:
                # Create a callback function that converts Zenoh samples to FluxMQ messages
                async def sample_handler(sample):
                    try:
                        # Extract data from the sample
                        data = sample.value.payload
                        
                        # Extract headers from attachments if any
                        headers = {}
                        for key, val in sample.value.attachment_items():
                            headers[key.decode()] = val.decode()
                        
                        # Create a FluxMQ message
                        message = Message(
                            topic=sample.key_expr.to_string(),
                            data=data,
                            headers=headers
                        )
                        
                        # Call the handler in a thread to avoid blocking the event loop
                        self._loop.run_in_executor(None, handler, message)
                    except Exception as e:
                        self._logger.error(f"Error in message handler for topic {topic}: {str(e)}")
                        self._logger.debug(f"Exception details: {traceback.format_exc()}")
                
                # Run the subscribe coroutine in the event loop
                future = asyncio.run_coroutine_threadsafe(
                    self._session.declare_subscriber(topic, sample_handler),
                    self._loop
                )
                
                # Wait for the subscription to complete
                subscriber = future.result(timeout=10)
                
                # Generate a unique subscription ID
                subscription_id = f"sub_{id(subscriber)}"
                self._subscriptions[subscription_id] = subscriber
                
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
            ConnectionError: If not connected to the Zenoh network
            ValueError: If the subscription ID is invalid
        """
        with self._lock:
            if not self._connected or self._session is None or self._loop is None:
                raise ConnectionError("Not connected to Zenoh network")
                
            if subscription_id not in self._subscriptions:
                self._logger.warning(f"Attempted to unsubscribe from unknown subscription: {subscription_id}")
                return False
                
            try:
                # Get the subscriber
                subscriber = self._subscriptions[subscription_id]
                
                # Run the unsubscribe coroutine in the event loop
                future = asyncio.run_coroutine_threadsafe(
                    subscriber.undeclare(),
                    self._loop
                )
                
                # Wait for the unsubscribe to complete
                future.result(timeout=10)
                
                # Remove from our tracking
                del self._subscriptions[subscription_id]
                
                self._logger.debug(f"Unsubscribed from subscription: {subscription_id}")
                return True
            except Exception as e:
                self._logger.error(f"Failed to unsubscribe from subscription {subscription_id}: {str(e)}")
                self._logger.debug(f"Exception details: {traceback.format_exc()}")
                raise 
    
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
            ConnectionError: If not connected to the Zenoh network
            TimeoutError: If the request times out
            ValueError: If the topic or data is invalid
        """
        with self._lock:
            if not self._connected or self._session is None or self._loop is None:
                raise ConnectionError("Not connected to Zenoh network")
                
            try:
                # Generate a unique correlation ID for this request
                correlation_id = str(uuid.uuid4())
                
                # Create a temporary response topic
                response_topic = f"{topic}/response/{correlation_id}"
                
                # Create headers with correlation ID if not provided
                request_headers = headers.copy() if headers else {}
                request_headers["correlation-id"] = correlation_id
                request_headers["reply"] = response_topic
                
                # Create a future to receive the response
                response_future = asyncio.Future()
                
                # Run the request coroutine in the event loop
                future = asyncio.run_coroutine_threadsafe(
                    self._request(topic, data, response_topic, correlation_id, request_headers, response_future, timeout),
                    self._loop
                )
                
                # Wait for the request to complete
                try:
                    response = future.result(timeout=timeout + 1)  # Add 1 second buffer
                    return response
                except concurrent.futures.TimeoutError:
                    self._logger.error(f"Request to topic {topic} timed out after {timeout} seconds")
                    raise TimeoutError(f"Request to topic {topic} timed out after {timeout} seconds")
            except Exception as e:
                if isinstance(e, TimeoutError):
                    raise
                self._logger.error(f"Failed to send request to topic {topic}: {str(e)}")
                self._logger.debug(f"Exception details: {traceback.format_exc()}")
                raise
    
    async def _request(self, topic: str, data: Any, response_topic: str, correlation_id: str, 
                       headers: Dict[str, str], response_future: asyncio.Future, timeout: float) -> Message:
        """Internal coroutine to send a request and wait for a response."""
        # Subscribe to the response topic
        async def response_handler(message):
            # Check if this is the response we're waiting for
            msg_correlation_id = message.headers.get("correlation-id")
            if msg_correlation_id == correlation_id and not response_future.done():
                response_future.set_result(message)
        
        sub_id = None
        try:
            # Subscribe to the response topic
            subscriber = await self._session.declare_subscriber(response_topic, response_handler)
            sub_id = f"sub_{id(subscriber)}"
            self._subscriptions[sub_id] = subscriber
            
            # Send the request
            await self._publish(topic, data if isinstance(data, bytes) else 
                               (data.encode('utf-8') if isinstance(data, str) else 
                                json.dumps(data).encode('utf-8')), headers)
            
            # Wait for the response with timeout
            try:
                response = await asyncio.wait_for(response_future, timeout)
                return response
            except asyncio.TimeoutError:
                raise TimeoutError(f"Request to topic {topic} timed out after {timeout} seconds")
        finally:
            # Clean up the temporary subscription
            if sub_id and sub_id in self._subscriptions:
                subscriber = self._subscriptions[sub_id]
                await subscriber.undeclare()
                del self._subscriptions[sub_id]
    
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
            ConnectionError: If not connected to the Zenoh network
            ValueError: If the message has no reply topic or the data is invalid
        """
        with self._lock:
            if not self._connected or self._session is None or self._loop is None:
                raise ConnectionError("Not connected to Zenoh network")
                
            # Get the reply topic from the request message
            reply_topic = request_message.headers.get("reply")
            if not reply_topic:
                raise ValueError("Cannot respond to a message without a reply topic")
                
            try:
                # Get the correlation ID from the request message
                correlation_id = request_message.headers.get("correlation-id")
                
                # Create headers with correlation ID if not provided
                response_headers = headers.copy() if headers else {}
                if correlation_id:
                    response_headers["correlation-id"] = correlation_id
                
                # Run the publish coroutine in the event loop
                future = asyncio.run_coroutine_threadsafe(
                    self._publish(reply_topic, data if isinstance(data, bytes) else 
                                 (data.encode('utf-8') if isinstance(data, str) else 
                                  json.dumps(data).encode('utf-8')), response_headers),
                    self._loop
                )
                
                # Wait for the publish to complete
                future.result(timeout=10)
                
                self._logger.debug(f"Sent response to reply topic: {reply_topic}")
                return True
            except Exception as e:
                self._logger.error(f"Failed to send response: {str(e)}")
                self._logger.debug(f"Exception details: {traceback.format_exc()}")
                raise 

class ZenohTopic(StandardTopic):
    """
    Zenoh implementation of the Topic interface.
    
    This class provides a standardized topic naming convention for Zenoh.
    Zenoh uses hierarchical key expressions for topics, which aligns well
    with the StandardTopic implementation.
    """
    
    def __init__(self, prefix: str = ""):
        """
        Initialize a new ZenohTopic.
        
        Args:
            prefix: Optional prefix to prepend to all topics
        """
        super().__init__(prefix)
    
    def _make_topic(self, *parts: str) -> str:
        """
        Create a topic string from parts.
        
        Args:
            *parts: Parts of the topic path
            
        Returns:
            A topic string with parts joined by '/'
        """
        # Filter out empty parts
        filtered_parts = [p for p in parts if p]
        
        # Join with '/'
        if self._prefix:
            return f"{self._prefix}/{'/'.join(filtered_parts)}"
        else:
            return '/'.join(filtered_parts)


class ZenohStatus(StandardStatus):
    """
    Zenoh implementation of the Status interface.
    
    This class provides standard status values for Zenoh services.
    """
    
    def __init__(self):
        """Initialize a new ZenohStatus."""
        super().__init__() 