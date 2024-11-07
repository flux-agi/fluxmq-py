from abc import abstractmethod, ABC


class NodeState(ABC):
    @abstractmethod
    def stopped(self):
        pass

    @abstractmethod
    def started(self):
        pass
