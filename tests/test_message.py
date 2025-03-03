"""
Tests for the Message class.
"""
import pytest
from fluxmq.message import Message


def test_message_init():
    """Test Message initialization with different data types."""
    # Test with dict data
    msg1 = Message(topic="test/topic", data={"key": "value"})
    assert msg1.topic == "test/topic"
    assert msg1.data == {"key": "value"}
    assert msg1.headers == {}
    assert msg1.reply is None

    # Test with string data
    msg2 = Message(topic="test/topic", data="test data")
    assert msg2.topic == "test/topic"
    assert msg2.data == "test data"
    
    # Test with bytes data
    msg3 = Message(topic="test/topic", data=b"test data")
    assert msg3.topic == "test/topic"
    assert msg3.data == b"test data"
    
    # Test with reply topic
    msg4 = Message(topic="test/topic", data="test", reply="reply/topic")
    assert msg4.topic == "test/topic"
    assert msg4.reply == "reply/topic"
    
    # Test with headers
    msg5 = Message(topic="test/topic", data="test", headers={"content-type": "text/plain"})
    assert msg5.headers == {"content-type": "text/plain"}


def test_message_get_data_methods():
    """Test the get_data_as_string and get_data_as_bytes methods."""
    # Test with string data
    msg1 = Message(topic="test/topic", data="test data")
    assert msg1.get_data_as_string() == "test data"
    assert msg1.get_data_as_bytes() == b"test data"
    
    # Test with dict data
    msg2 = Message(topic="test/topic", data={"key": "value"})
    assert msg2.get_data_as_string() == '{"key": "value"}'
    assert isinstance(msg2.get_data_as_bytes(), bytes)
    
    # Test with bytes data
    msg3 = Message(topic="test/topic", data=b"test data")
    assert msg3.get_data_as_string() == "test data"
    assert msg3.get_data_as_bytes() == b"test data"


def test_message_header_methods():
    """Test the header manipulation methods."""
    msg = Message(topic="test/topic", data="test")
    
    # Test add_header
    msg.add_header("content-type", "text/plain")
    assert msg.headers == {"content-type": "text/plain"}
    
    # Test get_header
    assert msg.get_header("content-type") == "text/plain"
    assert msg.get_header("non-existent") is None
    assert msg.get_header("non-existent", "default") == "default"
    
    # Test adding multiple headers
    msg.add_header("x-custom", "value")
    assert msg.headers == {"content-type": "text/plain", "x-custom": "value"}


def test_message_str_representation():
    """Test the string representation of a Message."""
    msg = Message(
        topic="test/topic", 
        data={"key": "value"}, 
        reply="reply/topic",
        headers={"content-type": "application/json"}
    )
    str_repr = str(msg)
    
    # Check that the string representation contains all the important information
    assert "test/topic" in str_repr
    assert "reply/topic" in str_repr
    assert "key" in str_repr
    assert "value" in str_repr
    assert "content-type" in str_repr 