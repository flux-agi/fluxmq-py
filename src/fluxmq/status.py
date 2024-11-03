from enum import Enum
from enum import IntFlag


class SStatus(Enum):
    READY = "ready"
    STOPPED = "stopped"
    RUNNING = "running"


class DStatus(IntFlag):
    READY = 1  # 0b0001
    STOPPED = 2  # 0b0010
    RUNNING = 4  # 0b0100
    ERROR = 8  # 0b1000
