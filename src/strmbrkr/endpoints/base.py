from __future__ import annotations
from abc import ABC, abstractclassmethod
from enum import Enum, auto


class EndpointSpecification(ABC):
    """Encapsulate the endpoints used across the ``strmbrkr`` package."""

    class Connection(Enum):
        """Enumerations of types of connections for ZMQ."""
        BIND = auto()
        CONNECT = auto()


    @abstractclassmethod
    def getBrokerProducerQueue(cls, connection: EndpointSpecification.Connection) -> str:
        """Returns the endpoint address for job submission.

        Args:
            connection (Connection): The type of connection that will use the resultant endpoint.

        Returns:
            str: Endpoint address that can be passed directly to :meth:`.zmq.Socket.bind()` (when `connection` is set to
                :attr:`.Connection.BIND`) or :meth:`.zmq.Socket.connect()` (when `connection` is set to
                :attr:`.Connection.CONNECT`).
        """
        raise NotImplementedError()

    @abstractclassmethod
    def getBrokerConsumerQueue(cls, connection: EndpointSpecification.Connection) -> str:
        """Returns the endpoint address for processed job consumption.

        Args:
            connection (Connection): The type of connection that will use the resultant endpoint.

        Returns:
            str: Endpoint address that can be passed directly to :meth:`.zmq.Socket.bind()` (when `connection` is set to
                :attr:`.Connection.BIND`) or :meth:`.zmq.Socket.connect()` (when `connection` is set to
                :attr:`.Connection.CONNECT`).
        """
        raise NotImplementedError()

    @abstractclassmethod
    def getBrokerBackend(cls, connection: EndpointSpecification.Connection) -> str:
        """Returns the broker backend endpoint string.

        Args:
            connection (Connection): The type of connection that will use the resultant endpoint.

        Returns:
            str: Endpoint address that can be passed directly to :meth:`.zmq.Socket.bind()` (when `connection` is set to
                :attr:`.Connection.BIND`) or :meth:`.zmq.Socket.connect()` (when `connection` is set to
                :attr:`.Connection.CONNECT`).
        """
        raise NotImplementedError()

    @abstractclassmethod
    def getKeyValueStore(cls, connection: EndpointSpecification.Connection) -> str:
        """Returns the key value store endpoint string.

        Args:
            connection (Connection): The type of connection that will use the resultant endpoint.

        Returns:
            str: Endpoint address that can be passed directly to :meth:`.zmq.Socket.bind()` (when `connection` is set to
                :attr:`.Connection.BIND`) or :meth:`.zmq.Socket.connect()` (when `connection` is set to
                :attr:`.Connection.CONNECT`).
        """
        raise NotImplementedError()
