# Standard Library Imports
from abc import ABC, abstractmethod
from typing import Any


class Transaction(ABC):
    """Represents a single unit of work "transaction" with the :class:`.KeyValueStore`."""

    SERVER_SHUTDOWN_KEY = "__SERVER_SHUTDOWN__"
    """str: Key used to flag the :class:`.KeyValueStore.Server`."""

    def __init__(self, key: str, request_payload: Any = None):
        """Initialize a :class:`.Transaction` instance.

        Args:
            key (str): Key to transact with in the :class:`.KeyValueStore`.
            request_payload (Any, optional): User-specified value used in resultant transaction.
        """
        self.key = key
        if self.key == self.SERVER_SHUTDOWN_KEY and not self._allowShutdown():
            raise ValueError("This Transaction is not allowed to shut down the server.")
        self.request_payload = request_payload
        self.response_payload = None
        self.error = None

    def _allowShutdown(self):
        """Return a boolean indication of whether this :class:`.Transaction` subclass is allowed to shut down the :class:`.KeyValueStore.Server`.

        Returns:
            bool: Indication of whether this :class:`.Transaction` subclass is allowed to shut down the :class:`.KeyValueStore.Server`
        """
        return False

    @abstractmethod
    def transact(self, key_value_store: dict):
        """Execute the transaction encapsulated by this :class:`.Transaction` on the specified `key_value_store`.

        Args:
            key_value_store (dict): Key value store to execute the encapsulated transaction on.
        """
        raise NotImplementedError()

    def getResponse(self):
        """Returns the response payload of this executed transaction.
        
        Returns:
            Any: Response payload of this executed transaction.

        Raises:
            Exception: If an error occurred while executing this transaction.
        """
        if self.error:
            raise self.error
        return self.response_payload

    VALUE_LIMIT =  100
    """int: Number of characters to limit the logging of values to."""

    def __repr__(self):
        """Return a human readable string representation of this :class:`.Transaction`."""
        return f"{self.__class__.__name__}(" + \
            f"key={self.key}, " + \
            f"request_payload={repr(self.request_payload)[:self.VALUE_LIMIT]}, " + \
            f"response_payload={repr(self.response_payload)[:self.VALUE_LIMIT]}, " + \
            f"error={self.error})"
