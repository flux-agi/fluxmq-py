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
from fluxmq.transport import Transport, SyncTransport, current_headers

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
            
            # Merge headers with context headers
            merged_headers = current_headers.get().copy()
            if headers:
                merged_headers.update(headers)
            
            # Create a value with headers if provided
            value = zenoh.Value(payload, encoding=zenoh.Encoding.APP_OCTET_STREAM)
            
            # Add headers as attachments
            if merged_headers:
                for key, val in merged_headers.items():
                    value.put_attachment(key.encode(), val.encode())
            
            # Publish the message
            await self._session.put(topic, value)
            self._logger.debug(f"Published message to topic: {topic}", extra={"headers": merged_headers})
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
                        payload=data,
                        reply=None,  # Zenoh doesn't have built-in reply subjects
                        headers=headers
                    )
                    
                    # Set context headers for propagation
                    token = current_headers.set(headers)
                    try:
                        # Call the user's handler
                        await handler(message)
                    finally:
                        # Reset context headers
                        current_headers.reset(token)
                        
                except Exception as e:
                    self._logger.error(f"Error handling message for topic {sample.key_expr}: {str(e)}")
                    self._logger.debug(f"Exception details: {traceback.format_exc()}")
            
            # Subscribe to the topic
            subscriber = await self._session.declare_subscriber(topic, sample_handler)
            subscription_id = str(uuid.uuid4())
            self._subscriptions[subscription_id] = subscriber
            self._logger.debug(f"Subscribed to topic: {topic}, ID: {subscription_id}")
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
        
        This implements the request-reply pattern using Zenoh queryables.
        
        Args:
            topic: The topic to send the request to
            data: The request data
            timeout: Timeout in seconds
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
            # Create a unique correlation ID
            correlation_id = str(uuid.uuid4())
            
            # Create a future to hold the response
            response_future = asyncio.Future()
            self._response_futures[correlation_id] = response_future
            
            # Merge headers with context headers
            merged_headers = current_headers.get().copy()
            if headers:
                merged_headers.update(headers)
            
            # Add correlation ID to headers
            merged_headers['correlation-id'] = correlation_id
            
            # Set up a selector for the response
            response_selector = f"{topic}/responses/{correlation_id}"
            
            # Define a handler for responses
            async def response_handler(message):
                # Check if this is the response we're waiting for
                if message.headers.get('correlation-id') == correlation_id:
                    if not response_future.done():
                        response_future.set_result(message)
            
            # Subscribe to responses
            response_sub_id = await self.subscribe(response_selector, response_handler)
            
            try:
                # Publish the request with headers
                await self.publish(f"{topic}/requests", data, merged_headers)
                
                # Wait for the response with a timeout
                try:
                    response = await asyncio.wait_for(response_future, timeout)
                    return response
                except asyncio.TimeoutError:
                    raise TimeoutError(f"Request to {topic} timed out after {timeout} seconds")
            finally:
                # Clean up
                await self.unsubscribe(response_sub_id)
                if correlation_id in self._response_futures:
                    del self._response_futures[correlation_id]
                    
        except Exception as e:
            if not isinstance(e, TimeoutError):
                self._logger.error(f"Failed to send request to {topic}: {str(e)}")
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
            ValueError: If the message or data is invalid
        """
        if not self._connected or self._session is None:
            raise ConnectionError("Not connected to Zenoh network")
            
        try:
            # Extract the correlation ID from the request
            correlation_id = request_message.headers.get('correlation-id')
            if not correlation_id:
                raise ValueError("Cannot respond to a request without a correlation ID")
            
            # Determine the response topic
            # Extract the base topic from the request topic (remove '/requests' suffix)
            request_topic = request_message.topic if hasattr(request_message, 'topic') else None
            if not request_topic:
                raise ValueError("Cannot respond to a request without a topic")
                
            base_topic = request_topic.replace('/requests', '')
            response_topic = f"{base_topic}/responses/{correlation_id}"
            
            # Merge headers with context headers and request headers
            merged_headers = current_headers.get().copy()
            # Include the original correlation ID in the response
            merged_headers['correlation-id'] = correlation_id
            if request_message.headers:
                # Copy relevant headers from request
                for key, value in request_message.headers.items():
                    if key != 'correlation-id':  # Don't duplicate correlation ID
                        merged_headers[f'request-{key}'] = value
            if headers:
                merged_headers.update(headers)
            
            # Publish the response
            await self.publish(response_topic, data, merged_headers)
            return True
            
        except Exception as e:
            self._logger.error(f"Failed to respond to request: {str(e)}")
            self._logger.debug(f"Exception details: {traceback.format_exc()}")
            raise

class SyncZenoh(SyncTransport):
    """
    Synchronous Zenoh implementation of the Transport interface.
    
    This class provides a synchronous interface to the Zenoh messaging system.
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
        self._loop = None
        self._thread = None
        
        if logger is None:
            self._logger = getLogger("fluxmq.zenoh")
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
        if self._connected:
            self._logger.warning("Already connected to Zenoh network")
            return True
            
        try:
            # Create and start an event loop in a separate thread
            self._loop = asyncio.new_event_loop()
            self._thread = threading.Thread(target=self._run_event_loop, daemon=True)
            self._thread.start()
            
            # Connect to Zenoh in the event loop
            future = asyncio.run_coroutine_threadsafe(self._connect(), self._loop)
            future.result(timeout=10)  # Wait for connection with timeout
            
            self._connected = True
            self._logger.debug("Connected to Zenoh network")
            return True
        except Exception as e:
            self._logger.error(f"Failed to connect to Zenoh network: {str(e)}")
            self._logger.debug(f"Exception details: {traceback.format_exc()}")
            
            # Clean up if connection failed
            if self._loop and self._loop.is_running():
                self._loop.call_soon_threadsafe(self._loop.stop)
            
            if self._thread and self._thread.is_alive():
                self._thread.join(timeout=1.0)
                
            raise ConnectionError(f"Failed to connect to Zenoh network: {str(e)}") from e
    
    async def _connect(self) -> None:
        """
        Internal async method to connect to Zenoh.
        """
        self._session = await zenoh.open(self._config)
    
    def _run_event_loop(self) -> None:
        """
        Run the event loop in a separate thread.
        """
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()
        
        # Clean up when the loop stops
        pending = asyncio.all_tasks(self._loop)
        if pending:
            self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
    
    def close(self) -> bool:
        """
        Close the connection to the Zenoh network.
        
        Returns:
            True if the connection was closed successfully
        """
        if not self._connected or self._session is None:
            self._logger.warning("Not connected to Zenoh network")
            return True
            
        try:
            # Close the session in the event loop
            future = asyncio.run_coroutine_threadsafe(self._close(), self._loop)
            future.result(timeout=10)  # Wait for close with timeout
            
            # Stop the event loop
            if self._loop and self._loop.is_running():
                self._loop.call_soon_threadsafe(self._loop.stop)
            
            # Wait for the thread to finish
            if self._thread and self._thread.is_alive():
                self._thread.join(timeout=1.0)
            
            self._connected = False
            self._logger.debug("Closed connection to Zenoh network")
            return True
        except Exception as e:
            self._logger.error(f"Error closing Zenoh connection: {str(e)}")
            self._logger.debug(f"Exception details: {traceback.format_exc()}")
            return False
    
    async def _close(self) -> None:
        """
        Internal async method to close the Zenoh session.
        """
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
        if not self._connected or self._session is None:
            raise ConnectionError("Not connected to Zenoh network")
            
        try:
            # Run the publish operation in the event loop
            future = asyncio.run_coroutine_threadsafe(
                self._publish(topic, data, headers), self._loop
            )
            future.result(timeout=10)  # Wait for publish with timeout
            return True
        except Exception as e:
            self._logger.error(f"Failed to publish message to topic {topic}: {str(e)}")
            self._logger.debug(f"Exception details: {traceback.format_exc()}")
            raise
    
    async def _publish(self, topic: str, data: Any, headers: Optional[Dict[str, str]] = None) -> None:
        """
        Internal async method to publish a message.
        """
        # Convert data to the appropriate format
        payload = data
        if not isinstance(payload, bytes):
            if isinstance(payload, str):
                payload = payload.encode('utf-8')
            else:
                # Convert to JSON
                payload = json.dumps(payload).encode('utf-8')
        
        # Merge headers with context headers
        merged_headers = current_headers.get().copy()
        if headers:
            merged_headers.update(headers)
        
        # Create a value with headers
        value = zenoh.Value(payload, encoding=zenoh.Encoding.APP_OCTET_STREAM)
        
        # Add headers as attachments
        if merged_headers:
            for key, val in merged_headers.items():
                value.put_attachment(key.encode(), val.encode())
        
        # Publish the message
        await self._session.put(topic, value)
        self._logger.debug(f"Published message to topic: {topic}", extra={"headers": merged_headers})
    
    def subscribe(self, topic: str, handler: Callable[[Message], None]) -> str:
        """
        Subscribe to a topic with a message handler.
        
        Args:
            topic: The topic to subscribe to
            handler: Function that will be called with each message
            
        Returns:
            A subscription ID that can be used to unsubscribe
            
        Raises:
            ConnectionError: If not connected to the Zenoh network
            ValueError: If the topic is invalid
        """
        if not self._connected or self._session is None:
            raise ConnectionError("Not connected to Zenoh network")
            
        try:
            # Create an async wrapper for the handler if it's not already async
            if asyncio.iscoroutinefunction(handler):
                async_handler = handler
            else:
                async def async_handler(message):
                    handler(message)
            
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
                        payload=data,
                        reply=None,  # Zenoh doesn't have built-in reply subjects
                        headers=headers
                    )
                    
                    # Set context headers for propagation
                    token = current_headers.set(headers)
                    try:
                        # Call the user's handler
                        await async_handler(message)
                    finally:
                        # Reset context headers
                        current_headers.reset(token)
                        
                except Exception as e:
                    self._logger.error(f"Error handling message for topic {sample.key_expr}: {str(e)}")
                    self._logger.debug(f"Exception details: {traceback.format_exc()}")
            
            # Subscribe to the topic in the event loop
            future = asyncio.run_coroutine_threadsafe(
                self._session.declare_subscriber(topic, sample_handler), self._loop
            )
            subscriber = future.result(timeout=10)  # Wait for subscribe with timeout
            
            # Generate a unique subscription ID
            subscription_id = str(uuid.uuid4())
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
            subscription_id: The subscription ID to unsubscribe
            
        Returns:
            True if the unsubscribe was successful
            
        Raises:
            ConnectionError: If not connected to the Zenoh network
            ValueError: If the subscription ID is invalid
        """
        if not self._connected or self._session is None:
            raise ConnectionError("Not connected to Zenoh network")
            
        if subscription_id not in self._subscriptions:
            self._logger.warning(f"Subscription not found: {subscription_id}")
            return False
            
        try:
            # Unsubscribe in the event loop
            future = asyncio.run_coroutine_threadsafe(
                self._subscriptions[subscription_id].undeclare(), self._loop
            )
            future.result(timeout=10)  # Wait for unsubscribe with timeout
            
            # Remove from subscriptions
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
        
        This implements the request-reply pattern using Zenoh queryables.
        
        Args:
            topic: The topic to send the request to
            data: The request data
            timeout: Timeout in seconds
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
            # Create a future to hold the async result
            outer_future = concurrent.futures.Future()
            
            # Execute the request in the event loop
            task_future = asyncio.run_coroutine_threadsafe(
                self._request(topic, data, headers, timeout, outer_future), self._loop
            )
            
            # Wait for the response with timeout
            try:
                return outer_future.result(timeout=timeout + 1.0)  # Add 1 second buffer
            finally:
                # Make sure the task completes even if we time out
                task_future.cancel()
                
        except concurrent.futures.TimeoutError:
            raise TimeoutError(f"Request to {topic} timed out after {timeout} seconds")
        except Exception as e:
            if not isinstance(e, TimeoutError):
                self._logger.error(f"Failed to send request to {topic}: {str(e)}")
                self._logger.debug(f"Exception details: {traceback.format_exc()}")
            raise
    
    async def _request(self, topic: str, data: Any, headers: Optional[Dict[str, str]], 
                      timeout: float, outer_future: concurrent.futures.Future) -> None:
        """
        Internal async method to send a request and set the result in the outer future.
        """
        try:
            # Create a unique correlation ID
            correlation_id = str(uuid.uuid4())
            
            # Merge headers with context headers
            merged_headers = current_headers.get().copy()
            if headers:
                merged_headers.update(headers)
            
            # Add correlation ID to headers
            merged_headers['correlation-id'] = correlation_id
            
            # Set up a selector for the response
            response_selector = f"{topic}/responses/{correlation_id}"
            
            # Create a future to hold the response
            response_future = asyncio.Future()
            
            # Define a handler for responses
            async def response_handler(message):
                # Check if this is the response we're waiting for
                if message.headers.get('correlation-id') == correlation_id:
                    if not response_future.done():
                        response_future.set_result(message)
            
            # Subscribe to responses
            response_sub_id = await self.subscribe(response_selector, response_handler)
            
            try:
                # Publish the request with headers
                await self._publish(f"{topic}/requests", data, merged_headers)
                
                # Wait for the response with a timeout
                try:
                    response = await asyncio.wait_for(response_future, timeout)
                    outer_future.set_result(response)
                except asyncio.TimeoutError:
                    if not outer_future.done():
                        outer_future.set_exception(TimeoutError(f"Request to {topic} timed out after {timeout} seconds"))
            finally:
                # Clean up
                await self._subscriptions[response_sub_id].undeclare()
                if response_sub_id in self._subscriptions:
                    del self._subscriptions[response_sub_id]
                    
        except Exception as e:
            if not outer_future.done():
                outer_future.set_exception(e)
    
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
            ValueError: If the message or data is invalid
        """
        if not self._connected or self._session is None:
            raise ConnectionError("Not connected to Zenoh network")
            
        try:
            # Execute the respond operation in the event loop
            future = asyncio.run_coroutine_threadsafe(
                self._respond(request_message, data, headers), self._loop
            )
            future.result(timeout=10)  # Wait for respond with timeout
            return True
        except Exception as e:
            self._logger.error(f"Failed to respond to request: {str(e)}")
            self._logger.debug(f"Exception details: {traceback.format_exc()}")
            raise
    
    async def _respond(self, request_message: Message, data: Any, headers: Optional[Dict[str, str]]) -> None:
        """
        Internal async method to respond to a request.
        """
        # Extract the correlation ID from the request
        correlation_id = request_message.headers.get('correlation-id')
        if not correlation_id:
            raise ValueError("Cannot respond to a request without a correlation ID")
        
        # Determine the response topic
        # Extract the base topic from the request topic (remove '/requests' suffix)
        request_topic = request_message.topic if hasattr(request_message, 'topic') else None
        if not request_topic:
            raise ValueError("Cannot respond to a request without a topic")
            
        base_topic = request_topic.replace('/requests', '')
        response_topic = f"{base_topic}/responses/{correlation_id}"
        
        # Merge headers with context headers and request headers
        merged_headers = current_headers.get().copy()
        # Include the original correlation ID in the response
        merged_headers['correlation-id'] = correlation_id
        if request_message.headers:
            # Copy relevant headers from request
            for key, value in request_message.headers.items():
                if key != 'correlation-id':  # Don't duplicate correlation ID
                    merged_headers[f'request-{key}'] = value
        if headers:
            merged_headers.update(headers)
        
        # Publish the response
        await self._publish(response_topic, data, merged_headers)

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