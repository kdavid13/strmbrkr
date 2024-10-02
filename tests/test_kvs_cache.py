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


def test_initCache(kvs_server_teardown):
    init_cache = InitCache("my_cache")
    assert KeyValueStore.submitTransaction(init_cache)


def test_duplicateInitCache(kvs_server_teardown):
    init_cache = InitCache("my_cache")
    assert KeyValueStore.submitTransaction(init_cache)

    with pytest.raises(ValueAlreadySet, match="Value already set for key"):
        KeyValueStore.submitTransaction(init_cache)

    init_cache.clear_existing = True
    assert KeyValueStore.submitTransaction(init_cache)
