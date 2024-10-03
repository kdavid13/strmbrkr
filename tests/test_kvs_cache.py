"""Test the functionality of the ``cache_transactions`` module."""
# Third Party Imports
import pytest

# Package Imports
from strmbrkr import KeyValueStore
from strmbrkr.key_value_store.cache_transactions import (
    InitCache,
    CachePut,
    CacheGrab,
    CacheMiss,
    UninitializedCache,
    ValueAlreadySet
)


CACHE_NAME = "my_cache"
KEY = "my_key"
VALUE = "my_value"


def test_initCache(kvs_server_teardown):
    init_cache = InitCache(CACHE_NAME)
    assert KeyValueStore.submitTransaction(init_cache)


def test_duplicateInitCache(kvs_server_teardown):
    init_cache = InitCache(CACHE_NAME)
    assert KeyValueStore.submitTransaction(init_cache)

    with pytest.raises(ValueAlreadySet, match="Value already set for key"):
        KeyValueStore.submitTransaction(init_cache)

    init_cache.clear_existing = True
    assert KeyValueStore.submitTransaction(init_cache)


def test_cachePutGrab(kvs_server_teardown):
    init_cache = InitCache(CACHE_NAME)
    assert KeyValueStore.submitTransaction(init_cache)

    cache_put = CachePut(CACHE_NAME, KEY, VALUE)
    assert KeyValueStore.submitTransaction(cache_put)

    cache_grab = CacheGrab(CACHE_NAME, KEY)
    grabbed = KeyValueStore.submitTransaction(cache_grab)

    assert grabbed == VALUE


def test_noCachePut(kvs_server_teardown):
    cache_put = CachePut(CACHE_NAME, KEY, VALUE)
    with pytest.raises(UninitializedCache):
        KeyValueStore.submitTransaction(cache_put)


def test_noCacheGrab(kvs_server_teardown):
    cache_grab = CacheGrab(CACHE_NAME, KEY)
    with pytest.raises(UninitializedCache):
        KeyValueStore.submitTransaction(cache_grab)
    

def test_cacheMiss(kvs_server_teardown):
    cache_init = InitCache(CACHE_NAME)
    KeyValueStore.submitTransaction(cache_init)

    cache_grab = CacheGrab(CACHE_NAME, KEY)
    with pytest.raises(CacheMiss):
        KeyValueStore.submitTransaction(cache_grab)


def test_cacheSize(kvs_server_teardown):
    cache_size = 16
    cache_init = InitCache(CACHE_NAME, max_size=cache_size)
    assert KeyValueStore.submitTransaction(cache_init)

    for index in range(cache_size * 2):
        cache_put = CachePut(CACHE_NAME, f"id_{index}", index)
        assert KeyValueStore.submitTransaction(cache_put)

    for index in range(cache_size):
        cache_grab = CacheGrab(CACHE_NAME, f"id_{index}")
        with pytest.raises(CacheMiss):
            KeyValueStore.submitTransaction(cache_grab)

        upper_index = cache_size + index
        cache_grab = CacheGrab(CACHE_NAME, f"id_{upper_index}")
        assert KeyValueStore.submitTransaction(cache_grab) == upper_index

