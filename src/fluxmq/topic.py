from abc import ABC, abstractmethod
from typing import Optional, List


class Topic(ABC):
    """
    Abstract base class for topic naming conventions.
    
    This class defines the interface for topic factories that can
    generate standardized topic names for different messaging patterns.
    """

    @abstractmethod
    def status(self, service_id: str) -> str:
        """
        Get the topic for service status updates.
        
        This topic is used by a service to publish its current status.
        
        Args:
            service_id: The ID of the service
            
        Returns:
            The topic string
        """
        pass

    @abstractmethod
    def set_service_state(self, service_id: str) -> str:
        """
        Get the topic for setting service state.
        
        This topic is used to publish updates to a service's state.
        
        Args:
            service_id: The ID of the service
            
        Returns:
            The topic string
        """
        pass

    @abstractmethod
    def get_service_state(self, service_id: str) -> str:
        """
        Get the topic for retrieving service state.
        
        This topic is used to subscribe to a service's state updates.
        
        Args:
            service_id: The ID of the service
            
        Returns:
            The topic string
        """
        pass

    @abstractmethod
    def get_common_data(self, service_id: str) -> str:
        """
        Get the topic for retrieving common data from a service.
        
        Args:
            service_id: The ID of the service
            
        Returns:
            The topic string
        """
        pass
    
    @abstractmethod
    def ide_status(self) -> str:
        """
        Get the topic for IDE status updates.
        
        Returns:
            The topic string
        """
        pass
    
    @abstractmethod
    def set_common_data(self, service_id: str) -> str:
        """
        Get the topic for setting common data for a service.
        
        Args:
            service_id: The ID of the service
            
        Returns:
            The topic string
        """
        pass
    
    @abstractmethod
    def restart_node(self, node_id: str) -> str:
        """
        Get the topic for restarting a node.
        
        This topic is used to send restart commands to a node.
        
        Args:
            node_id: The ID of the node
            
        Returns:
            The topic string
        """
        pass

    @abstractmethod
    def dev_mode(self, service_id: str) -> str:
        """
        Get the topic for development mode settings.
        
        Args:
            service_id: The ID of the service
            
        Returns:
            The topic string
        """
        pass

    @abstractmethod
    def configuration(self, service_id: str) -> str:
        """
        Get the topic for service configuration.
        
        This topic is used to send configuration to a service.
        The service should start after receiving configuration.
        
        Args:
            service_id: The ID of the service
            
        Returns:
            The topic string
        """
        pass
    
    @abstractmethod
    def node_settings(self, node_id: str) -> str:
        """
        Get the topic for node settings.
        
        This topic is used to send settings to a specific node.
        
        Args:
            node_id: The ID of the node
            
        Returns:
            The topic string
        """
        pass

    @abstractmethod
    def node_created(self, node_id: str) -> str:
        """
        Get the topic for node creation notifications.
        
        Args:
            node_id: The ID of the node
            
        Returns:
            The topic string
        """
        pass

    @abstractmethod
    def service_settings(self, service_id: str) -> str:
        """
        Get the topic for service settings.
        
        This topic is used to send settings to a service.
        
        Args:
            service_id: The ID of the service
            
        Returns:
            The topic string
        """
        pass

    @abstractmethod
    def start(self, service_id: str) -> str:
        """
        Get the topic for starting a service.
        
        This topic is used to send start commands to a service.
        The node_id is passed in the payload and can be a wildcard '*'.
        
        Args:
            service_id: The ID of the service
            
        Returns:
            The topic string
        """
        pass

    @abstractmethod
    def stop(self, service_id: str) -> str:
        """
        Get the topic for stopping a service.
        
        This topic is used to send stop commands to a service.
        The node_id is passed in the payload and can be a wildcard '*'.
        
        Args:
            service_id: The ID of the service
            
        Returns:
            The topic string
        """
        pass

    @abstractmethod
    def time(self) -> str:
        """
        Get the topic for time synchronization.
        
        This topic is used by the manager to send time synchronization messages.
        
        Returns:
            The topic string
        """
        pass

    @abstractmethod
    def configuration_request(self, service_id: str) -> str:
        """
        Get the topic for requesting configuration.
        
        This topic is used by a service to request its configuration from the manager.
        
        Args:
            service_id: The ID of the service
            
        Returns:
            The topic string
        """
        pass

    @abstractmethod
    def status_request(self, service_id: str) -> str:
        """
        Get the topic for requesting status.
        
        This topic is used to request the current status of a service.
        The service should respond by publishing to the status topic.
        
        Args:
            service_id: The ID of the service
            
        Returns:
            The topic string
        """
        pass

    @abstractmethod
    def node_state_request(self, service_id: str) -> str:
        """
        Get the topic for requesting node state.
        
        This topic is used to request the current state of all nodes in a service.
        
        Args:
            service_id: The ID of the service
            
        Returns:
            The topic string
        """
        pass

    @abstractmethod
    def node_status(self, node_id: str) -> str:
        """
        Get the topic for node status updates.
        
        This topic is used by a node to publish its current status.
        
        Args:
            node_id: The ID of the node
            
        Returns:
            The topic string
        """
        pass

    @abstractmethod
    def error(self, service_id: str) -> str:
        """
        Get the topic for error messages.
        
        This topic is used to publish and subscribe to error messages.
        
        Args:
            service_id: The ID of the service
            
        Returns:
            The topic string
        """
        pass


class StandardTopic(Topic):
    """
    Standard implementation of the Topic interface.
    
    This class provides a concrete implementation of the Topic interface
    using a standardized topic naming convention.
    """
    
    def __init__(self, prefix: Optional[str] = None):
        """
        Initialize a new StandardTopic.
        
        Args:
            prefix: Optional prefix to prepend to all topics
        """
        self.prefix = prefix or ""
        if self.prefix and not self.prefix.endswith('/'):
            self.prefix += '/'
    
    def _make_topic(self, parts: List[str]) -> str:
        """
        Create a topic string from parts.
        
        Args:
            parts: List of topic parts
            
        Returns:
            The complete topic string
        """
        return f"{self.prefix}{'/'.join(parts)}"
    
    def status(self, service_id: str) -> str:
        return self._make_topic(["service", service_id, "status"])
    
    def set_service_state(self, service_id: str) -> str:
        return self._make_topic(["service", service_id, "state"])
    
    def get_service_state(self, service_id: str) -> str:
        return self._make_topic(["service", service_id, "state"])
    
    def get_common_data(self, service_id: str) -> str:
        return self._make_topic(["service", service_id, "get_common_data"])
    
    def ide_status(self) -> str:
        return self._make_topic(["ide", "status"])
    
    def set_common_data(self, service_id: str) -> str:
        return self._make_topic(["service", service_id, "common_data"])
    
    def restart_node(self, node_id: str) -> str:
        return self._make_topic(["node", node_id, "restart"])
    
    def dev_mode(self, service_id: str) -> str:
        return self._make_topic(["service", service_id, "dev_mode"])
    
    def configuration(self, service_id: str) -> str:
        return self._make_topic(["service", service_id, "config"])
    
    def node_settings(self, node_id: str) -> str:
        return self._make_topic(["node", node_id, "settings"])
    
    def node_created(self, node_id: str) -> str:
        return self._make_topic(["node", node_id, "created"])
    
    def service_settings(self, service_id: str) -> str:
        return self._make_topic(["service", service_id, "settings"])
    
    def start(self, service_id: str) -> str:
        return self._make_topic(["service", service_id, "start"])
    
    def stop(self, service_id: str) -> str:
        return self._make_topic(["service", service_id, "stop"])
    
    def time(self) -> str:
        return self._make_topic(["system", "time"])
    
    def configuration_request(self, service_id: str) -> str:
        return self._make_topic(["service", service_id, "request_config"])
    
    def status_request(self, service_id: str) -> str:
        return self._make_topic(["service", service_id, "request_status"])
    
    def node_state_request(self, service_id: str) -> str:
        return self._make_topic(["service", service_id, "request_node_state"])
    
    def node_status(self, node_id: str) -> str:
        return self._make_topic(["node", node_id, "status"])
    
    def error(self, service_id: str) -> str:
        return self._make_topic(["service", service_id, "error"])

