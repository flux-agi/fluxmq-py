from abc import ABC, abstractmethod


class TopicFactory(ABC):
    @abstractmethod
    def status(self, service_id: str):
        pass

    @abstractmethod
    def node_state(self, node_id: str):
        pass

    @abstractmethod
    def configuration(self, service_id: str):
        pass

    @abstractmethod
    def control(self, service_id: str):
        pass

    @abstractmethod
    def start(self, service_id: str):
        pass

    @abstractmethod
    def stop(self, service_id: str):
        pass

    @abstractmethod
    def time(self):
        pass

    @abstractmethod
    def configuration_request(self, service_id: str):
        pass

    @abstractmethod
    def status_request(self, service_id: str):
        pass

    @abstractmethod
    def node_state_request(self, service_id: str):
        pass

