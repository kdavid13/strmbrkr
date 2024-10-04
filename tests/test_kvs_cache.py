"""Test the functionality of the ``cache_transactions`` module."""
# Third Party Imports
import pytest

# Package Imports
from strmbrkr import KeyValueStore
from strmbrkr.key_value_store.cache_transactions import (
    CacheMiss,
    UninitializedCache,
    ValueAlreadySet
)


CACHE_NAME = "my_cache"
KEY = "my_key"
VALUE = "my_value"


def test_initCache(kvs_server_teardown):
    assert KeyValueStore.initCache(CACHE_NAME)


def test_duplicateInitCache(kvs_server_teardown):
    assert KeyValueStore.initCache(CACHE_NAME)

    with pytest.raises(ValueAlreadySet, match="Value already set for key"):
        KeyValueStore.initCache(CACHE_NAME)

    assert KeyValueStore.initCache(CACHE_NAME, clear_existing=True)


def test_cachePutGrab(kvs_server_teardown):
    assert KeyValueStore.initCache(CACHE_NAME)

    assert KeyValueStore.cachePut(CACHE_NAME, KEY, VALUE)

    grabbed = KeyValueStore.cacheGrab(CACHE_NAME, KEY)
    assert grabbed == VALUE


def test_noCachePut(kvs_server_teardown):
    with pytest.raises(UninitializedCache):
        KeyValueStore.cachePut(CACHE_NAME, KEY, VALUE)


def test_noCacheGrab(kvs_server_teardown):
    with pytest.raises(UninitializedCache):
        KeyValueStore.cacheGrab(CACHE_NAME, KEY)

 
def test_notCachePut(kvs_server_teardown):
    KeyValueStore.setValue(CACHE_NAME, VALUE)
    with pytest.raises(UninitializedCache):
        KeyValueStore.cachePut(CACHE_NAME, KEY, VALUE)


def test_notCacheGrab(kvs_server_teardown):
    KeyValueStore.setValue(CACHE_NAME, VALUE)
    with pytest.raises(UninitializedCache):
        KeyValueStore.cacheGrab(CACHE_NAME, KEY)


def test_cacheMiss(kvs_server_teardown):
    assert KeyValueStore.initCache(CACHE_NAME)

    with pytest.raises(CacheMiss):
        KeyValueStore.cacheGrab(CACHE_NAME, KEY)


def test_cacheSize(kvs_server_teardown):
    cache_size = 16
    KeyValueStore.initCache(CACHE_NAME, max_size=cache_size)

    for index in range(cache_size * 2):
        assert KeyValueStore.cachePut(CACHE_NAME, f"id_{index}", index)

    for index in range(cache_size):
        with pytest.raises(CacheMiss):
            KeyValueStore.cacheGrab(CACHE_NAME, f"id_{index}")

        upper_index = cache_size + index
        assert KeyValueStore.cacheGrab(CACHE_NAME, f"id_{upper_index}") == upper_index

