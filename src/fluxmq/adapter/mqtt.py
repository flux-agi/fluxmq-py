from asyncio import Queue

from fluxmq.message import Message
from fluxmq.node import NodeFactory, Node
from fluxmq.node import NodeState
from fluxmq.service import Service
from fluxmq.status import Status
from fluxmq.topic import Topic
from fluxmq.transport import Transport


class MQTT(Transport):
    async def close(self) -> None:
        pass

    async def connect(self) -> None:
        pass

    async def publish(self, topic: str, payload):
        pass

    async def subscribe(self, topic: str) -> Queue[Message]:
        pass

    async def unsubscribe(self, topic: str):
        pass

    async def request(self, topic: str, payload):
        pass

    async def respond(self, topic: str, response):
        pass


class MQTTTopic(Topic):
    def set_node_state(self, node_id: str):
        pass

    def start(self, service_id: str):
        pass

    def stop(self, service_id: str):
        pass

    def node_state_request(self, service_id: str):
        pass

    def configuration_request(self, service_id: str):
        pass

    def status_request(self, service_id: str):
        pass

    def request_configuration(self, service_id: str):
        return f""

    def time(self):
        return f"time"

    def control(self, service_id: str):
        return f"service/{service_id}/control"

    def status(self, service_id: str):
        return f"service/{service_id}/status"

    def configuration(self, service_id: str):
        return f"service/{service_id}/configuration"
    
    def error(self, message: str) -> None:
        """Handle errors in the MQTTTopic"""
        print(f"Error: {message}")
        pass


class MQTTStatus(Status):
    def up(self):
        return "up"

    def down(self):
        return "down"


class MQTTNodeState(NodeState):

    def stopped(self):
        return "stopped"

    def started(self):
        return "started"


class MQTTNodeFactory(NodeFactory):
    def create_node(self, service: Service) -> Node:
        return Node(logger=service.logger,
                    service=service,
                    state_factory=MQTTNodeState())
