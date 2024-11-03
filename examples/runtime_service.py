from examples.command import Command
from fluxmq.message import Message
from fluxmq.service import Service
from fluxmq.status import DStatus
import json


class RuntimeService(Service):
    def on_configuration(self, message: Message):
        configuration = json.loads(message.payload.encode())
        # create runtime with configuration and start
        self.send_status(DStatus.READY | DStatus.RUNNING)
        pass

    def on_control(self, message: Message):
        command = json.loads(message.payload.encode())
        if command == Command.START:
            # start runtime
            if message.reply:
                self.respond(message, DStatus.READY | DStatus.RUNNING)
        if command == Command.STOP:
            # stop runtime
            if message.reply:
                self.respond(message, DStatus.READY | DStatus.STOPPED)
        return

    def on_shutdown(self, signal_number, frame):
        pass

    def on_time(self, time: int):
        self.logger.debug(f"System coordinated time: {time}")
        pass
