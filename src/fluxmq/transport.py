from abc import ABC, abstractmethod
from asyncio.queues import Queue
from typing import Callable, Optional, Union, TypeVar, Generic, Any, Awaitable, Dict
import contextvars

from fluxmq.message import Message

# Context variable to track current headers from received messages
current_headers = contextvars.ContextVar('current_headers', default={})

T = TypeVar('T')
MessageType = TypeVar('MessageType', bound=Message)

class TypedQueue(Queue, Generic[T]):
    """
    A typed queue for handling specific message types.
    
    This extends the standard asyncio Queue with generic type information.
    """
    pass


class Transport(ABC):
    """
    Abstract base class for message transport implementations.
    
    This class defines the interface for asynchronous messaging transports
    that can connect to a messaging system, publish and subscribe to topics,
    and handle request-reply patterns.
    """
    
    @abstractmethod
    async def connect(self) -> None:
        """
        Connect to the messaging system.
        
        This method should establish a connection to the underlying
        messaging system and prepare it for use.
        
        Raises:
            ConnectionError: If connection to the messaging system fails
        """
        pass

    @abstractmethod
    async def close(self) -> None:
        """
        Close the connection to the messaging system.
        
        This method should properly clean up resources and close
        the connection to the messaging system.
        """
        pass

    @abstractmethod
    async def publish(self, topic: str, payload: Union[bytes, str], headers: Optional[Dict[str, str]] = None) -> None:
        """
        Publish a message to a topic.
        
        Args:
            topic: The topic to publish to
            payload: The message payload to publish
            headers: Optional headers to include with the message. If not provided,
                     headers from the current message context will be used.
            
        Raises:
            ConnectionError: If not connected to the messaging system
            ValueError: If the topic or payload is invalid
        """
        pass

    @abstractmethod
    async def subscribe(self, topic: str, handler: Callable[[Message], Awaitable[None]]) -> Any:
        """
        Subscribe to a topic with a message handler.
        
        Args:
            topic: The topic to subscribe to
            handler: Async callback function that will be called with each message
            
        Returns:
            A subscription identifier or object that can be used to unsubscribe
            
        Raises:
            ConnectionError: If not connected to the messaging system
            ValueError: If the topic is invalid
        """
        pass

    @abstractmethod
    async def unsubscribe(self, topic: str) -> None:
        """
        Unsubscribe from a topic.
        
        Args:
            topic: The topic to unsubscribe from
            
        Raises:
            ConnectionError: If not connected to the messaging system
            ValueError: If the topic is invalid or not subscribed
        """
        pass

    @abstractmethod
    async def request(self, topic: str, payload: Union[bytes, str], headers: Optional[Dict[str, str]] = None) -> Message:
        """
        Send a request and wait for a response.
        
        This implements the request-reply pattern, sending a message to a topic
        and waiting for a response.
        
        Args:
            topic: The topic to send the request to
            payload: The request payload
            headers: Optional headers to include with the request. If not provided,
                     headers from the current message context will be used.
            
        Returns:
            The response message
            
        Raises:
            ConnectionError: If not connected to the messaging system
            TimeoutError: If the request times out
            ValueError: If the topic or payload is invalid
        """
        pass

    @abstractmethod
    async def respond(self, message: Message, response: Union[bytes, str], headers: Optional[Dict[str, str]] = None) -> None:
        """
        Respond to a request message.
        
        Args:
            message: The request message to respond to
            response: The response data
            headers: Optional headers to include with the response. If not provided,
                     headers from the current message context will be used.
            
        Raises:
            ConnectionError: If not connected to the messaging system
            ValueError: If the message or response is invalid
        """
        pass


class SyncTransport(ABC):
    """
    Abstract base class for synchronous message transport implementations.
    
    This class defines the interface for synchronous messaging transports
    that can connect to a messaging system, publish and subscribe to topics,
    and handle request-reply patterns without using asyncio.
    """
    
    @abstractmethod
    def connect(self) -> None:
        """
        Connect to the messaging system.
        
        This method should establish a connection to the underlying
        messaging system and prepare it for use.
        
        Raises:
            ConnectionError: If connection to the messaging system fails
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        Close the connection to the messaging system.
        
        This method should properly clean up resources and close
        the connection to the messaging system.
        """
        pass

    @abstractmethod
    def publish(self, topic: str, payload: Union[bytes, str], headers: Optional[Dict[str, str]] = None) -> None:
        """
        Publish a message to a topic.
        
        Args:
            topic: The topic to publish to
            payload: The message payload to publish
            headers: Optional headers to include with the message. If not provided,
                     headers from the current message context will be used.
            
        Raises:
            ConnectionError: If not connected to the messaging system
            ValueError: If the topic or payload is invalid
        """
        pass

    @abstractmethod
    def subscribe(self, topic: str, callback: Callable[[Message], None]) -> Any:
        """
        Subscribe to a topic with a message handler.
        
        Args:
            topic: The topic to subscribe to
            callback: Function that will be called with each message
            
        Returns:
            A subscription identifier or object that can be used to unsubscribe
            
        Raises:
            ConnectionError: If not connected to the messaging system
            ValueError: If the topic is invalid
        """
        pass

    @abstractmethod
    def unsubscribe(self, topic: str) -> None:
        """
        Unsubscribe from a topic.
        
        Args:
            topic: The topic to unsubscribe from
            
        Raises:
            ConnectionError: If not connected to the messaging system
            ValueError: If the topic is invalid or not subscribed
        """
        pass

    @abstractmethod
    def request(self, topic: str, payload: Union[bytes, str], headers: Optional[Dict[str, str]] = None) -> Message:
        """
        Send a request and wait for a response.
        
        This implements the request-reply pattern, sending a message to a topic
        and waiting for a response.
        
        Args:
            topic: The topic to send the request to
            payload: The request payload
            headers: Optional headers to include with the request. If not provided,
                     headers from the current message context will be used.
            
        Returns:
            The response message
            
        Raises:
            ConnectionError: If not connected to the messaging system
            TimeoutError: If the request times out
            ValueError: If the topic or payload is invalid
        """
        pass

    @abstractmethod
    def respond(self, message: Message, response: Union[bytes, str], headers: Optional[Dict[str, str]] = None) -> None:
        """
        Respond to a request message.
        
        Args:
            message: The request message to respond to
            response: The response data
            headers: Optional headers to include with the response. If not provided,
                     headers from the current message context will be used.
            
        Raises:
            ConnectionError: If not connected to the messaging system
            ValueError: If the message or response is invalid
        """
        pass