
from typing import Any

from .transaction import abbreviateStr, Transaction


class ValueAlreadySet(Exception):
    """Error raised by :class:`.InitCache` if cache key already has a value set."""

    def __init__(self, key: str, value: Any):
        msg = f"Value already set for key '{key}': {abbreviateStr(repr(value))}"
        super().__init__(msg)


class UninitializedCache(Exception):
    """Error raised by :class:`.CacheGrab` if the cache being retrieved from doesn't exist."""

    def __init__(self, cache_name: str):
        msg = f"Cache '{cache_name}' has not been initialized."
        super().__init__(msg)


class CacheMiss(Exception):
    """Error raised by :class:`.CacheGrab` if the value being retrieved doesn't exist."""

    def __init__(self, cache_name: str, key: str):
        msg = f"Cache '{cache_name}' does not have a populated value for key '{key}'"
        super().__init__(msg)


class InitCache(Transaction):
    """Transaction encapsulating the initialization of a cache."""

    def __init__(self, cache_name: str, clear_existing: bool = False):
        """
        Args:
            cache_name: Name of cache to initialize.
            clear_existing: Flag indicating whether to clear an existing cache. Default is
                ``False``, resulting in an error being raised if a cache already exists for
                `cache_name`.
        """
        super().__init__(cache_name)
        self.cache_name = cache_name
        self.clear_existing = clear_existing

    def transact(self, key_value_store: dict):
        if (value := key_value_store.get(self.cache_name)) is not None:
            if not self.clear_existing:
                self.error = ValueAlreadySet(self.cache_name, value)
                return

        key_value_store[self.cache_name] = dict()


class CachePut(Transaction):
    """Populate a value in a specified cache."""

    def __init__(self, cache_name: str, identifier: str, value: Any):
        """
        Args:
            cache_name: Name of cache to put specified value into.
            identifier: Unique identifier for specified value.
            value: Value being put into the specified cache.
        """
        super().__init__(cache_name)
        self.cache_name = cache_name
        self.identifier = identifier
        self.value = value

    def transact(self, key_value_store: dict):
        try:
            cache = key_value_store[self.cache_name]
        except KeyError:
            self.error = UninitializedCache(self.key)
            return

        cache[self.identifier] = self.value


class CacheGrab(Transaction):
    """Attempt to retrieve a specified value from a specified cache."""

    def __init__(self, cache_name: str, identifier: str):
        """
        Args:
            cache_name: Name of cache to grab value stored in `identifier` from.
            identifier: Unique identifier of value stored in cache to grab.
        """
        super().__init__(cache_name)
        self.identifier = identifier

    def transact(self, key_value_store: dict):
        try:
            cache = key_value_store[self.key]
        except KeyError:
            self.error = UninitializedCache(self.key)
            return

        try:
            self.response_payload = cache[self.identifier]
        except KeyError:
            self.error = CacheMiss(self.key, self.identifier)
            return
