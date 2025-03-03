"""
Tests for the Status module.
"""
import pytest
from fluxmq.status import ServiceStatusEnum, NodeStatusEnum, StatusFlags, StandardStatus


def test_service_status_enum():
    """Test the ServiceStatusEnum values."""
    assert ServiceStatusEnum.UNKNOWN == "unknown"
    assert ServiceStatusEnum.STARTING == "starting"
    assert ServiceStatusEnum.RUNNING == "running"
    assert ServiceStatusEnum.STOPPING == "stopping"
    assert ServiceStatusEnum.STOPPED == "stopped"
    assert ServiceStatusEnum.ERROR == "error"


def test_node_status_enum():
    """Test the NodeStatusEnum values."""
    assert NodeStatusEnum.UNKNOWN == "unknown"
    assert NodeStatusEnum.ONLINE == "online"
    assert NodeStatusEnum.OFFLINE == "offline"
    assert NodeStatusEnum.BUSY == "busy"
    assert NodeStatusEnum.ERROR == "error"


def test_status_flags():
    """Test the StatusFlags class."""
    # Test individual flags
    assert StatusFlags.NONE.value == 0
    assert StatusFlags.ERROR.value == 1
    assert StatusFlags.WARNING.value == 2
    assert StatusFlags.INFO.value == 4
    
    # Test combining flags
    combined = StatusFlags.ERROR | StatusFlags.WARNING
    assert combined.value == 3
    assert StatusFlags.ERROR in combined
    assert StatusFlags.WARNING in combined
    assert StatusFlags.INFO not in combined
    
    # Test adding flags
    flags = StatusFlags.NONE
    flags |= StatusFlags.ERROR
    assert flags.value == 1
    flags |= StatusFlags.INFO
    assert flags.value == 5


def test_standard_status_init():
    """Test StandardStatus initialization."""
    status = StandardStatus()
    assert status._status == ServiceStatusEnum.UNKNOWN


def test_standard_status_unknown():
    """Test the unknown method."""
    status = StandardStatus()
    result = status.unknown()
    assert result == ServiceStatusEnum.UNKNOWN


def test_standard_status_starting():
    """Test the starting method."""
    status = StandardStatus()
    result = status.starting()
    assert result == ServiceStatusEnum.STARTING


def test_standard_status_running():
    """Test the running method."""
    status = StandardStatus()
    result = status.running()
    assert result == ServiceStatusEnum.RUNNING


def test_standard_status_stopping():
    """Test the stopping method."""
    status = StandardStatus()
    result = status.stopping()
    assert result == ServiceStatusEnum.STOPPING


def test_standard_status_stopped():
    """Test the stopped method."""
    status = StandardStatus()
    result = status.stopped()
    assert result == ServiceStatusEnum.STOPPED


def test_standard_status_error():
    """Test the error method."""
    status = StandardStatus()
    
    # Test without message
    result1 = status.error()
    assert result1 == ServiceStatusEnum.ERROR
    
    # Test with message
    result2 = status.error("Test error message")
    assert result2 == ServiceStatusEnum.ERROR
    # The message should be stored internally but not affect the return value


def test_standard_status_as_dict():
    """Test the as_dict method."""
    status = StandardStatus()
    
    # Test with simple status
    status.running()
    result1 = status.as_dict()
    assert result1 == {"status": "running"}
    
    # Test with error status and message
    status.error("Test error message")
    result2 = status.as_dict()
    assert result2 == {"status": "error", "message": "Test error message"} 