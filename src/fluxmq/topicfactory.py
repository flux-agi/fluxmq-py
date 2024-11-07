from abc import ABC, abstractmethod


class TopicFactory(ABC):
    @abstractmethod
    def status(self, service_id: str):
        pass

    @abstractmethod
    def node_status(self, node_id: str):
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
    def send_configuration(self, service_id: str):
        pass

    @abstractmethod
    def send_status(self, service_id: str):
        pass

