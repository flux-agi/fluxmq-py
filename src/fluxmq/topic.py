from abc import ABC, abstractmethod


class Topic(ABC):
    """
    topic factory
    """

    @abstractmethod
    def status(self, service_id: str):
        """
        pub topic to be published by service with service current status
        :param service_id:
        """
        pass

    @abstractmethod
    def set_service_state(self, service_id: str):
        """
        pub topic to set node's state
        :param node_id:
        """
        pass

    @abstractmethod
    def get_service_state(self, service_id: str):
        """
        sub topic to get node's state
        :param node_id:
        """
        pass

    def restart_node(self, node_id: str):
        """
        sub topic for restart command for node
        """
        pass

    @abstractmethod
    def dev_mode(self, service_id: str):
        pass

    @abstractmethod
    def configuration(self, service_id: str):
        """
        sub topic to listen to by service, service received configuration in this topic
        should start immediately after configuration is received
        :param service_id:
        """
        pass
    
    @abstractmethod
    def node_settings(self, node_id: str):
        """
        sub topic to listen by service and get config of service.
        :param service_id:
        """
        pass

    @abstractmethod
    def service_settings(self, service_id: str):
        """
        sub topic to listen by service and get config of service.
        :param service_id:
        """
        pass

    @abstractmethod
    def start(self, service_id: str):
        """
        sub topic to listen to by service, service should start node|nodes upon message in this topic
        node_id is passed in the payload, can be wildcard '*'
        :param service_id:
        """
        pass

    @abstractmethod
    def stop(self, service_id: str):
        """
        sub topic to listen to by service, service should stop node|nodes upon message in this topic
        node_id is passed in the payload, can be wildcard '*'
        :param service_id:
        """
        pass

    @abstractmethod
    def time(self):
        """
        sub topic to listen to by service, manager sends here time synchronization
        """
        pass

    @abstractmethod
    def configuration_request(self, service_id: str):
        """
        pub topic to get configuration from manager
        :param service_id:
        """
        pass

    @abstractmethod
    def status_request(self, service_id: str):
        """
        sub topic to listen to by service, should send back status in status() topic
        :param service_id:
        """
        pass

    @abstractmethod
    def node_state_request(self, service_id: str):
        """
        sub topic to listen to by service, should send back status of all nodes attached
        :param service_id:
        """
        pass

    @abstractmethod
    def error(self, service_id: str):
        """
        pub-sub topic to listen to, or to publish about the error
        """
        pass

