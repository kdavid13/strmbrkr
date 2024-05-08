from __future__ import annotations
# Standard Library Imports
from enum import Enum
from json import dump, load
from os.path import isdir, join
from os import makedirs
import socket
from time import sleep
# Package Imports
from .base import EndpointSpecification
from ..config import Config


class TCPEndpointSpecification(EndpointSpecification):
    """Encapsulate the endpoints used across the ``strmbrkr`` package."""

    class Endpoint(Enum):
        PRODUCER = "producer"
        CONSUMER = "consumer"
        BACKEND = "backend"
        KEY_VALUE_STORE = "key_value_store"

    ROOT_DIR = ".strmbrkr_tcp"
    """str: Root directory that where endpoint port definitions will live."""

    FILE_FORMAT = "{0}_ports.json"
    """str: Format string used to determine the name of the file that will hold the endpoint ports."""

    ENDPOINT_PREFIX = "tcp"
    """str: String prefix to specify that ``zmq`` should use the TCP protocol."""

    ADDR = {
        EndpointSpecification.Connection.BIND: "*",
        EndpointSpecification.Connection.CONNECT: "localhost"
    }
    """dict[Connection, str]: Address to use depending on connection type."""

    PORTS = None
    """dict | None: Dictionary holding the port numbers for the endpoints."""

    @classmethod
    def _getPort(cls, endpoint: TCPEndpointSpecification.Endpoint) -> dict:
        if cls.PORTS is None:
            file_name = cls.FILE_FORMAT.format(Config.socket.instance_id)
            port_path = join(cls.ROOT_DIR, file_name)

            if not isdir(cls.ROOT_DIR):
                makedirs(cls.ROOT_DIR, exist_ok=True)

            try:
                dump_file = open(port_path, 'x')
            except FileExistsError:
                sleep(0.25)
                with open(port_path, 'r') as port_file:
                    cls.PORTS = load(port_file)
            else:
                producer = socket.socket()
                producer.bind(('', 0))
                consumer = socket.socket()
                consumer.bind(('', 0))
                back = socket.socket()
                back.bind(('', 0))
                kvs = socket.socket()
                kvs.bind(('', 0))

                cls.PORTS = {
                    cls.Endpoint.PRODUCER.value: producer.getsockname()[1],
                    cls.Endpoint.CONSUMER.value: consumer.getsockname()[1],
                    cls.Endpoint.BACKEND.value: back.getsockname()[1],
                    cls.Endpoint.KEY_VALUE_STORE.value: kvs.getsockname()[1]
                }
                producer.close()
                consumer.close()
                back.close()
                kvs.close()
                dump(cls.PORTS, dump_file)
                dump_file.close()

        return cls.PORTS[endpoint.value]

    @classmethod
    def getBrokerProducerQueue(cls, connection: EndpointSpecification.Connection) -> str:
        """Returns the endpoint address for job submission."""
        return f"{cls.ENDPOINT_PREFIX}://{cls.ADDR[connection]}:{cls._getPort(cls.Endpoint.PRODUCER)}"

    @classmethod
    def getBrokerConsumerQueue(cls, connection: EndpointSpecification.Connection) -> str:
        """Returns the endpoint address for processed job consumption."""
        return f"{cls.ENDPOINT_PREFIX}://{cls.ADDR[connection]}:{cls._getPort(cls.Endpoint.CONSUMER)}"

    @classmethod
    def getBrokerBackend(cls, connection: EndpointSpecification.Connection) -> str:
        """Returns the broker backend endpoint string."""
        return f"{cls.ENDPOINT_PREFIX}://{cls.ADDR[connection]}:{cls._getPort(cls.Endpoint.BACKEND)}"

    @classmethod
    def getKeyValueStore(cls, connection: EndpointSpecification.Connection) -> str:
        """Returns the key value store endpoint string."""
        return f"{cls.ENDPOINT_PREFIX}://{cls.ADDR[connection]}:{cls._getPort(cls.Endpoint.KEY_VALUE_STORE)}"
