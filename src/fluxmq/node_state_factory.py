from abc import abstractmethod, ABC


class NodeStateFactory(ABC):
    @abstractmethod
    def stopped(self):
        pass

    @abstractmethod
    def started(self):
        pass
