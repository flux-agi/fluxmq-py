"""
Tests for the Topic module.
"""
import pytest
from fluxmq.topic import StandardTopic


def test_standard_topic_init():
    """Test StandardTopic initialization with different prefixes."""
    # Test with no prefix
    topic1 = StandardTopic()
    assert topic1._prefix == ""
    
    # Test with prefix
    topic2 = StandardTopic(prefix="test")
    assert topic2._prefix == "test/"
    
    # Test with prefix that already has a trailing slash
    topic3 = StandardTopic(prefix="test/")
    assert topic3._prefix == "test/"


def test_standard_topic_status():
    """Test the status method."""
    topic = StandardTopic(prefix="test")
    
    # Test with service_id
    status_topic = topic.status("service1")
    assert status_topic == "test/status/service1"
    
    # Test without service_id
    status_topic_all = topic.status()
    assert status_topic_all == "test/status"


def test_standard_topic_set_service_state():
    """Test the set_service_state method."""
    topic = StandardTopic(prefix="test")
    
    state_topic = topic.set_service_state("service1")
    assert state_topic == "test/state/service1"


def test_standard_topic_get_service_state():
    """Test the get_service_state method."""
    topic = StandardTopic(prefix="test")
    
    state_topic = topic.get_service_state("service1")
    assert state_topic == "test/state/service1"


def test_standard_topic_get_common_data():
    """Test the get_common_data method."""
    topic = StandardTopic(prefix="test")
    
    data_topic = topic.get_common_data("key1")
    assert data_topic == "test/common/key1"


def test_standard_topic_set_common_data():
    """Test the set_common_data method."""
    topic = StandardTopic(prefix="test")
    
    data_topic = topic.set_common_data("key1")
    assert data_topic == "test/common/key1"


def test_standard_topic_service_command():
    """Test the service_command method."""
    topic = StandardTopic(prefix="test")
    
    # Test with specific service
    cmd_topic = topic.service_command("service1", "start")
    assert cmd_topic == "test/command/service1/start"
    
    # Test with all services
    cmd_topic_all = topic.service_command(command="stop")
    assert cmd_topic_all == "test/command/stop"


def test_standard_topic_node_command():
    """Test the node_command method."""
    topic = StandardTopic(prefix="test")
    
    # Test with specific node
    cmd_topic = topic.node_command("node1", "restart")
    assert cmd_topic == "test/node/node1/restart"
    
    # Test with all nodes
    cmd_topic_all = topic.node_command(command="shutdown")
    assert cmd_topic_all == "test/node/shutdown"


def test_standard_topic_node_status():
    """Test the node_status method."""
    topic = StandardTopic(prefix="test")
    
    # Test with specific node
    status_topic = topic.node_status("node1")
    assert status_topic == "test/node/node1/status"
    
    # Test with all nodes
    status_topic_all = topic.node_status()
    assert status_topic_all == "test/node/status" 