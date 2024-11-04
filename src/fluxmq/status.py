from enum import Enum
from enum import IntFlag


class Status(Enum):
    UP = "up"
    DOWN = "down"
    STARTED = "started"
    STOPPED = "stopped"


class DStatus(IntFlag):
    READY = 1  # 0b0001
    STOPPED = 2  # 0b0010
    RUNNING = 4  # 0b0100
    ERROR = 8  # 0b1000


"""
use like DStatus.READY | DStatus.STOPPED, DStatus.READY | DStatus.RUNNING
"""
