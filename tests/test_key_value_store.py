"""Test the functionality of the :class:`.KeyValueStore` class."""
# Third Party Imports
import pytest

# Package Imports
from strmbrkr import KeyValueStore


def test_setGetValue(kvs_server_teardown):
    assert KeyValueStore.setValue("foo", "bar") == "bar"
    assert KeyValueStore.getValue("foo") == "bar"


def test_appendValue(kvs_server_teardown):
    my_list = ["first element", "second_element"]
    assert KeyValueStore.appendValue("my_list", my_list[0]) == [my_list[0]]
    assert KeyValueStore.appendValue("my_list", my_list[1]) == my_list
    assert KeyValueStore.getValue("my_list") == my_list


def test_badAppendValue(kvs_server_teardown):
    assert KeyValueStore.setValue("foo", "bar") == "bar"
    with pytest.raises(TypeError):
        KeyValueStore.appendValue('foo', 'baz')


def test_popValue(kvs_server_teardown):
    my_list = ["first element", "second_element"]
    assert KeyValueStore.appendValue("my_list", my_list[0]) == [my_list[0]]
    assert KeyValueStore.appendValue("my_list", my_list[1]) == my_list

    assert KeyValueStore.popValue("my_list", 0) == my_list.pop(0)
    assert KeyValueStore.popValue("my_list", 0) == my_list.pop(0)
    assert KeyValueStore.popValue("my_list", 0) == None


def test_popValueNoExisting(kvs_server_teardown):
    assert KeyValueStore.popValue("does_not_exist", 0) == None


def test_popValueBadExisting(kvs_server_teardown):
    assert KeyValueStore.setValue("is_int", 123)
    with pytest.raises(TypeError):
        _ = KeyValueStore.popValue("is_int", 0)

def test_flush(kvs_server_teardown):
    assert KeyValueStore.setValue("is_int", 123)
    assert KeyValueStore.flush() == {}
    assert KeyValueStore.getValue("is_int") == None
