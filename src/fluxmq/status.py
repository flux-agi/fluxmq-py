from abc import ABC, abstractmethod
from enum import Enum, IntFlag
from typing import Dict, Any, Union

"""
Status module for defining service and node status values.

This module provides abstract base classes and concrete implementations
for handling status values in the messaging system.
"""

class ServiceStatusEnum(Enum):
    """
    Standard status values for services.
    
    These values represent the common states a service can be in.
    """
    CONNECTED = "connected"
    READY = "ready"
    ACTIVE = "active"
    PAUSED = "paused"
    ERROR = "error"
    STARTING = "starting"
    STOPPING = "stopping"
    STOPPED = "stopped"


class NodeStatusEnum(Enum):
    """
    Standard status values for nodes.
    
    These values represent the common states a node can be in.
    """
    CREATED = "created"
    INITIALIZED = "initialized"
    STARTING = "starting"
    STARTED = "started"
    RUNNING = "running"
    PAUSING = "pausing"
    PAUSED = "paused"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"


class StatusFlags(IntFlag):
    """
    Bit flags for representing multiple status values.
    
    These can be combined using bitwise operations to represent
    complex status states.
    
    Example:
        status = StatusFlags.READY | StatusFlags.RUNNING
    """
    NONE = 0
    READY = 1      # 0b0001
    STOPPED = 2    # 0b0010
    RUNNING = 4    # 0b0100
    ERROR = 8      # 0b1000
    PAUSED = 16    # 0b10000


class Status(ABC):
    """
    Abstract base class for status implementations.
    
    This class defines the interface for status objects that can
    provide standard status values for services.
    """

    @abstractmethod
    def connected(self) -> Union[str, Dict[str, Any]]:
        """
        Get the connected status value.
        
        Returns:
            The connected status value
        """
        pass

    @abstractmethod
    def ready(self) -> Union[str, Dict[str, Any]]:
        """
        Get the ready status value.
        
        Returns:
            The ready status value
        """
        pass

    @abstractmethod
    def active(self) -> Union[str, Dict[str, Any]]:
        """
        Get the active status value.
        
        Returns:
            The active status value
        """
        pass

    @abstractmethod
    def paused(self) -> Union[str, Dict[str, Any]]:
        """
        Get the paused status value.
        
        Returns:
            The paused status value
        """
        pass

    @abstractmethod
    def error(self) -> Union[str, Dict[str, Any]]:
        """
        Get the error status value.
        
        Returns:
            The error status value
        """
        pass
        
    @abstractmethod
    def starting(self) -> Union[str, Dict[str, Any]]:
        """
        Get the starting status value.
        
        Returns:
            The starting status value
        """
        pass
        
    @abstractmethod
    def stopping(self) -> Union[str, Dict[str, Any]]:
        """
        Get the stopping status value.
        
        Returns:
            The stopping status value
        """
        pass
        
    @abstractmethod
    def stopped(self) -> Union[str, Dict[str, Any]]:
        """
        Get the stopped status value.
        
        Returns:
            The stopped status value
        """
        pass


class StandardStatus(Status):
    """
    Standard implementation of the Status interface.
    
    This class provides a concrete implementation of the Status interface
    using the ServiceStatusEnum values.
    """
    
    def connected(self) -> str:
        """
        Get the connected status value.
        
        Returns:
            The connected status string
        """
        return ServiceStatusEnum.CONNECTED.value
        
    def ready(self) -> str:
        """
        Get the ready status value.
        
        Returns:
            The ready status string
        """
        return ServiceStatusEnum.READY.value
        
    def active(self) -> str:
        """
        Get the active status value.
        
        Returns:
            The active status string
        """
        return ServiceStatusEnum.ACTIVE.value
        
    def paused(self) -> str:
        """
        Get the paused status value.
        
        Returns:
            The paused status string
        """
        return ServiceStatusEnum.PAUSED.value
        
    def error(self) -> str:
        """
        Get the error status value.
        
        Returns:
            The error status string
        """
        return ServiceStatusEnum.ERROR.value
        
    def starting(self) -> str:
        """
        Get the starting status value.
        
        Returns:
            The starting status string
        """
        return ServiceStatusEnum.STARTING.value
        
    def stopping(self) -> str:
        """
        Get the stopping status value.
        
        Returns:
            The stopping status string
        """
        return ServiceStatusEnum.STOPPING.value
        
    def stopped(self) -> str:
        """
        Get the stopped status value.
        
        Returns:
            The stopped status string
        """
        return ServiceStatusEnum.STOPPED.value
        
    def as_dict(self, status: str) -> Dict[str, str]:
        """
        Convert a status string to a dictionary.
        
        Args:
            status: The status string
            
        Returns:
            A dictionary with the status value
        """
        return {"status": status}
