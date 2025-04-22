import asyncio
import threading
import nats
import json

from asyncio import Queue
from logging import Logger, getLogger
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from typing import Callable, Dict, List, TypeVar, Generic, Optional, Union, Any, Awaitable

from fluxmq.message import Message
from fluxmq.status import Status
from fluxmq.topic import Topic
from fluxmq.transport import Transport, SyncTransport, current_headers

MessageType = TypeVar('Message', bound=Message)

class TypedQueue(Queue, Generic[MessageType]):
    pass

class Nats(Transport):
    connection = None
    logger: Logger
    servers: List[str]
    subscriptions: Dict[str, Subscription]

    def __init__(self, servers: List[str], logger=None):
        self.servers = servers
        self.subscriptions = {}
        if logger is None:
            self.logger = getLogger()
        else:
            self.logger = logger

    async def connect(self) -> bool:
        try:
            self.connection = await nats.connect(servers=self.servers)
            self.logger.debug(f"Connected to {self.servers}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to NATS: {str(e)}")
            raise ConnectionError(f"Failed to connect to NATS: {str(e)}")

    async def publish(self, topic: str, payload: Union[bytes, str], headers: Optional[Dict[str, str]] = None) -> bool:
        if not isinstance(payload, bytes):
            if isinstance(payload, str):
                payload = payload.encode('utf-8')
            else:
                # Convert to JSON if not bytes or string
                payload = json.dumps(payload).encode('utf-8')
        
        # Merge headers with context headers, with provided headers taking precedence
        merged_headers = current_headers.get().copy()
        if headers:
            merged_headers.update(headers)
        
        # Only set headers if we have any
        if merged_headers:
            await self.connection.publish(topic, payload, headers=merged_headers)
        else:
            await self.connection.publish(topic, payload)
            
        self.logger.debug("Sent message", extra={"topic": topic, "payload": payload, "headers": merged_headers})
        return True

    async def subscribe(self, topic: str, handler: Callable[[Message], Awaitable[None]]) -> str:
        """
        Subscribe to a topic with a message handler.
        
        Args:
            topic: The topic to subscribe to
            handler: Async callback function that will be called with each message
            
        Returns:
            A subscription identifier that can be used to unsubscribe
        """
        
        async def message_handler(msg: Msg):
            # Extract headers from the message if they exist
            msg_headers = {}
            if hasattr(msg, 'headers') and msg.headers:
                msg_headers = dict(msg.headers)
            
            # Create a Message object
            message = Message(
                payload=msg.data,
                reply=msg.reply,
                headers=msg_headers
            )
            
            # Set context headers for propagation to subsequent publish operations
            token = current_headers.set(msg_headers)
            try:
                # Call user handler
                await handler(message)
            finally:
                # Reset context headers to previous state
                current_headers.reset(token)

        subscription = await self.connection.subscribe(topic, cb=message_handler)
        subscription_id = str(subscription.sid)
        self.subscriptions[subscription_id] = subscription
        self.logger.info(f"Subscribed to topic: {topic} with ID: {subscription_id}")
        return subscription_id

    async def unsubscribe(self, subscription_id: str) -> bool:
        if subscription_id in self.subscriptions:
            subscription = self.subscriptions[subscription_id]
            await subscription.unsubscribe()
            del self.subscriptions[subscription_id]
            self.logger.debug(f"Unsubscribed from subscription: {subscription_id}")
            return True
        else:
            self.logger.warning(f"Subscription not found: {subscription_id}")
            return False

    async def request(self, topic: str, payload: Union[bytes, str], headers: Optional[Dict[str, str]] = None) -> Message:
        if not isinstance(payload, bytes):
            if isinstance(payload, str):
                payload = payload.encode('utf-8')
            else:
                # Convert to JSON if not bytes or string
                payload = json.dumps(payload).encode('utf-8')
        
        # Merge headers with context headers, with provided headers taking precedence
        merged_headers = current_headers.get().copy()
        if headers:
            merged_headers.update(headers)
        
        # Send request with headers if available
        if merged_headers:
            response = await self.connection.request(topic, payload, headers=merged_headers)
        else:
            response = await self.connection.request(topic, payload)
        
        # Create response message with headers if available
        response_headers = {}
        if hasattr(response, 'headers') and response.headers:
            response_headers = dict(response.headers)
        
        return Message(
            payload=response.data,
            reply=response.reply,
            headers=response_headers
        )

    async def respond(self, message: Message, response: Union[bytes, str], headers: Optional[Dict[str, str]] = None) -> bool:
        if message.reply is None:
            self.logger.warning("Cannot respond: Message has no reply subject")
            return False
        
        if not isinstance(response, bytes):
            if isinstance(response, str):
                response = response.encode('utf-8')
            else:
                # Convert to JSON if not bytes or string
                response = json.dumps(response).encode('utf-8')
        
        # Merge headers with context headers and message headers, with provided headers taking precedence
        merged_headers = current_headers.get().copy()
        if message.headers:
            merged_headers.update(message.headers)
        if headers:
            merged_headers.update(headers)
        
        # Publish response with headers if available
        if merged_headers:
            await self.connection.publish(message.reply, response, headers=merged_headers)
        else:
            await self.connection.publish(message.reply, response)
            
        return True

    async def close(self) -> bool:
        if self.connection:
            await self.connection.close()
            self.connection = None
            self.logger.debug("Closed NATS connection")
            return True
        return False

class SyncNats(SyncTransport):
    connection = None
    logger: Logger
    servers: List[str]
    subscriptions: Dict[str, Any]

    def __init__(self, servers: List[str], logger=None):
        self.servers = servers
        self.subscriptions = {}
        self.nc = NATS()
        self.loop = None
        self.thread = None
        self.connected = False

        if logger is None:
            self.logger = getLogger()
        else:
            self.logger = logger

    def connect(self) -> bool:
        try:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.thread = threading.Thread(target=self._run_event_loop, daemon=True)
            self.thread.start()

            future = asyncio.run_coroutine_threadsafe(self.nc.connect(servers=self.servers), self.loop)
            future.result()
            self.connected = True
            self.logger.debug(f"Connected to {self.servers}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to NATS: {str(e)}")
            raise ConnectionError(f"Failed to connect to NATS: {str(e)}")

    def _run_event_loop(self):
        self.loop.run_forever()

    def publish(self, topic: str, payload: Union[bytes, str], headers: Optional[Dict[str, str]] = None) -> bool:
        if not self.connected:
            raise ConnectionError("Not connected to NATS")
            
        if not isinstance(payload, bytes):
            if isinstance(payload, str):
                payload = payload.encode('utf-8')
            else:
                # Convert to JSON if not bytes or string
                payload = json.dumps(payload).encode('utf-8')
        
        # Merge headers with context headers, with provided headers taking precedence
        merged_headers = current_headers.get().copy()
        if headers:
            merged_headers.update(headers)
        
        # Create publish coroutine with or without headers
        if merged_headers:
            publish_coro = self.nc.publish(topic, payload, headers=merged_headers)
        else:
            publish_coro = self.nc.publish(topic, payload)
            
        # Execute publish
        future = asyncio.run_coroutine_threadsafe(publish_coro, self.loop)
        future.result()  # Wait for the publish to complete
        
        self.logger.debug("Sent message", extra={
            "topic": topic, 
            "payload": payload,
            "headers": merged_headers
        })
        return True

    def subscribe(self, topic: str, callback: Callable[[Message], None]) -> str:
        if not self.connected:
            raise ConnectionError("Not connected to NATS")

        async def message_handler(msg: Msg):
            # Extract headers from the message if they exist
            msg_headers = {}
            if hasattr(msg, 'headers') and msg.headers:
                msg_headers = dict(msg.headers)
            
            # Create a Message object
            message = Message(
                payload=msg.data,
                reply=msg.reply,
                headers=msg_headers
            )
            
            # Set context headers for propagation to subsequent publish operations
            token = current_headers.set(msg_headers)
            try:
                # Call user handler
                if asyncio.iscoroutinefunction(callback):
                    await callback(message)
                else:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, callback, message)
            finally:
                # Reset context headers to previous state
                current_headers.reset(token)

        # Subscribe to the topic
        future = asyncio.run_coroutine_threadsafe(
            self.nc.subscribe(topic, cb=message_handler), self.loop
        )
        subscription = future.result()
        subscription_id = str(subscription.sid)
        
        # Store the subscription
        self.subscriptions[subscription_id] = subscription
        self.logger.debug(f"Subscribed to topic: {topic} with ID: {subscription_id}")
        
        return subscription_id

    def unsubscribe(self, subscription_id: str) -> bool:
        if not self.connected:
            raise ConnectionError("Not connected to NATS")
            
        if subscription_id not in self.subscriptions:
            self.logger.warning(f"Subscription not found: {subscription_id}")
            return False
            
        subscription = self.subscriptions[subscription_id]
        future = asyncio.run_coroutine_threadsafe(subscription.unsubscribe(), self.loop)
        future.result()  # Wait for the unsubscribe to complete
        
        del self.subscriptions[subscription_id]
        return True

    def close(self) -> bool:
        if not self.connected:
            self.logger.warning("Not connected to NATS")
            return True
            
        future = asyncio.run_coroutine_threadsafe(self.nc.close(), self.loop)
        future.result()  # Wait for the close to complete
        
        self.connected = False
        
        # Stop the event loop
        if self.loop and self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)
            
        # Wait for the thread to finish
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=1.0)
            
        self.logger.debug("Closed NATS connection")
        return True

    def request(self, topic: str, payload: Union[bytes, str], headers: Optional[Dict[str, str]] = None) -> Message:
        if not self.connected:
            raise ConnectionError("Not connected to NATS")
            
        if not isinstance(payload, bytes):
            if isinstance(payload, str):
                payload = payload.encode('utf-8')
            else:
                # Convert to JSON if not bytes or string
                payload = json.dumps(payload).encode('utf-8')
        
        # Merge headers with context headers, with provided headers taking precedence
        merged_headers = current_headers.get().copy()
        if headers:
            merged_headers.update(headers)
        
        # Create request coroutine with or without headers
        if merged_headers:
            request_coro = self.nc.request(topic, payload, headers=merged_headers)
        else:
            request_coro = self.nc.request(topic, payload)
            
        # Execute request
        future = asyncio.run_coroutine_threadsafe(request_coro, self.loop)
        response = future.result()  # Wait for the response
        
        # Extract headers from the response if they exist
        response_headers = {}
        if hasattr(response, 'headers') and response.headers:
            response_headers = dict(response.headers)
        
        # Create and return the response message
        return Message(
            payload=response.data,
            reply=response.reply,
            headers=response_headers
        )

    def respond(self, message: Message, response: Union[bytes, str], headers: Optional[Dict[str, str]] = None) -> bool:
        if not self.connected:
            raise ConnectionError("Not connected to NATS")
            
        if message.reply is None:
            self.logger.warning("Cannot respond: Message has no reply subject")
            return False
        
        if not isinstance(response, bytes):
            if isinstance(response, str):
                response = response.encode('utf-8')
            else:
                # Convert to JSON if not bytes or string
                response = json.dumps(response).encode('utf-8')
        
        # Merge headers with context headers and message headers, with provided headers taking precedence
        merged_headers = current_headers.get().copy()
        if message.headers:
            merged_headers.update(message.headers)
        if headers:
            merged_headers.update(headers)
        
        # Create publish coroutine with or without headers
        if merged_headers:
            publish_coro = self.nc.publish(message.reply, response, headers=merged_headers)
        else:
            publish_coro = self.nc.publish(message.reply, response)
            
        # Execute publish
        future = asyncio.run_coroutine_threadsafe(publish_coro, self.loop)
        future.result()  # Wait for the publish to complete
        
        return True

class NatsTopic(Topic):
    def set_service_state(self, service_id: str):
        return f"service.{service_id}.set_common_state"

    def get_service_state(self, service_id: str):
        return f"service.{service_id}.get_common_state"
    
    def get_common_data(self, service_id: str):
        return f"service.{service_id}.get_common_data"
    
    def set_common_data(self, service_id: str):
        return f"service.{service_id}.set_common_data"

    def ide_status(self):
        return f"ide.status"
    
    def start(self, service_id: str):
        return f"service.{service_id}.start"

    def stop(self, service_id: str):
        return f"service.{service_id}.stop"
    
    def restart_node(self, service_id: str):
        return f"service.{service_id}.restart"

    def node_status(self, node_id: str):
        return f"node.{node_id}.status"
    
    def node_state_request(self, node_id: str):
        return f"node.{node_id}.state_request"

    def dev_mode(self, service_id: str):
         return f"service.development_mode.{service_id}"

    def time(self):
        return "service.tick"

    def status(self, service_id: str):
        return f"service.{service_id}.status"

    def configuration(self, service_id: str):
        return f"service.{service_id}.set_config"
    
    def configuration_request(self, service_id: str):
        return f"service.{service_id}.get_config"
    
    def node_settings(self, node_id: str):
        return f"node.{node_id}.set_settings"
    
    def node_created(self, node_id: str):
        return f"node.{node_id}.created"
    
    def service_settings(self, service_id: str):
        return f"service.{service_id}.set_settings"

    def status_request(self, service_id: str):
        return f"service.{service_id}.request_status"

    def error(self, service_id: str):
        return f"service.{service_id}.error"


class NatsStatus(Status):
    def starting(self):
        return "STARTING"
    
    def connected(self):
        return "CONNECTED"

    def ready(self):
        return "READY"

    def active(self):
        return "ACTIVE"

    def paused(self):
        return "PAUSED"

    def error(self):
        return "ERROR"
        
    def stopping(self):
        return "STOPPING"
        
    def stopped(self):
        return "STOPPED"