
from copy import deepcopy
from operator import itemgetter
from time import time_ns
from typing import Any

from .transaction import abbreviateStr, Transaction


class ValueAlreadySet(Exception):
    """Error raised by :class:`.InitCache` if cache key already has a value set."""

    def __init__(self, key: str, value: Any):
        self.msg = f"Value already set for key '{key}': {abbreviateStr(repr(value))}"

    def __str__(self):
        return self.msg


class UninitializedCache(Exception):
    """Error raised by :class:`.CacheGrab` if the cache being retrieved from doesn't exist."""

    def __init__(self, cache_name: str):
        self.msg = f"Cache '{cache_name}' has not been initialized."

    def __str__(self):
        return self.msg


class CacheMiss(Exception):
    """Error raised by :class:`.CacheGrab` if the value being retrieved doesn't exist."""

    def __init__(self, cache_name: str, key: str):
        self.msg = f"Cache '{cache_name}' does not have a populated value for key '{key}'"

    def __str__(self):
        return self.msg


class Cache:
    """Encapsulation of cache functionality."""

    def __init__(self, cache_name: str):
        """
        Args:
            cache_name: Name of cache to initialize.
        """
        self.name = cache_name
        self._contents = dict()

    def getRecord(self, record_name: str) -> Any:
        """Retrieve a record from this cache.

        Args:
            record_name: Unique identifier for the record being retrieved.

        Returns:
            The value of the record being retrieved.

        Raises:
            CacheMiss: If the record does not exist in this cache.
        """
        try:
            record = self._contents[record_name]
        except KeyError:
            raise CacheMiss(self.name, record_name)
        return record

    def putRecord(self, record_name: str, record_value: Any):
        """Store a record in this cache.

        Args:
            record_name: Unique identifier for the record being retrieved.
            record_value: Value of record being stored.
        """
        self._contents[record_name] = record_value



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

        key_value_store[self.cache_name] = Cache(self.cache_name)

        self.response_payload = {
            "cache_name": self.cache_name,
            "clear_existing": self.clear_existing
        }


class CachePut(Transaction):
    """Store a record in a specified cache."""

    def __init__(self, cache_name: str, record_name: str, record_value: Any):
        """
        Args:
            cache_name: Name of cache to put the record into.
            record_name: Unique identifier for specified record.
            record_value: Value being put into the specified cache.
        """
        super().__init__(cache_name)
        self.cache_name = cache_name
        self.record_name = record_name
        self.record_value = record_value

    def transact(self, key_value_store: dict):
        try:
            cache: Cache = key_value_store[self.cache_name]
        except KeyError:
            self.error = UninitializedCache(self.cache_name)
            return

        cache.putRecord(self.record_name, self.record_value)

        self.response_payload = {
            "cache_name": self.cache_name,
            "record_name": self.record_name,
            "record_value": self.record_value
        }


class CacheGrab(Transaction):
    """Attempt to retrieve a specified record from a specified cache."""

    def __init__(self, cache_name: str, record_name: str):
        """
        Args:
            cache_name: Name of cache to grab the specified record from.
            record_name: Unique identifier of record to retrieve.
        """
        super().__init__(cache_name)
        self.cache_name = cache_name
        self.record_name = record_name

    def transact(self, key_value_store: dict):
        try:
            cache: Cache = key_value_store[self.cache_name]
        except KeyError:
            self.error = UninitializedCache(self.cache_name)
            return

        try:
            self.response_payload = cache.getRecord(self.record_name)
        except CacheMiss as miss:
            self.error = miss
            return
