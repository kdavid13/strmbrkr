
# Standard Library Imports
from os.path import isdir, join
from os import makedirs
# Package Imports
from .base import EndpointSpecification
from ..config import Config


class IPCEndpointSpecification(EndpointSpecification):
    """Encapsulate the endpoints used across the ``strmbrkr`` package.

    This encapsulation is necessary to ensure that the ``strmbrkr`` package works properly across multiple contexts.
    By default, each Python interpreter that imports and utilizes some aspect of the ``strmbrkr`` package will
    differentiate its IPC file handles by storing them in a subdirectory of :attr:`.ROOT_DIR` based on the string value
    stored in :attr:`.INSTANCE_ID` (which will default to the process ID (pid) of the process instantiating ``strmbrkr``
    components).
    Without this differentiation, the IPC file handles will be shared across ``strmbrkr`` API instances and can interfere
    with each other.
    The easiest way to exemplify this behavior is to reference a test designed specifically for this feature:
    ``tests/test_package.py::test_multipleWorkerManagers``.
    As the test is written, it passes; however, if one were to add ``EndpointSpecification.setInstanceId("foo")`` to 
    the top of ``example.py`` (along with the requisite import); the test will fail to complete.
    This seems to be mainly due to the key value store endpoint being shared across multiple instances of the API.
    While the broker endpoints seem to be able to handle the interference (perhaps because of their more complex
    ROUTER/DEALER ``zmq`` design pattern), the key value store(s) do not work as intended.
    """

    ROOT_DIR = ".strmbrkr_ipc"
    """str: Root directory that where collections of IPC file handles will live."""

    ENDPOINT_PREFIX = "ipc"
    """str: String prefix to specify that ``zmq`` should use the IPC protocol."""

    BROKER_PRODUCER_FILENAME = "producer.ipc"
    """str: Name of IPC file handle used for job submission."""

    BROKER_CONSUMER_FILENAME = "consumer.ipc"
    """str: ame of IPC file handle used for processed job consumption."""

    BROKER_BACKEND_FILENAME = "backend.ipc"
    """str: Name of IPC file handle used for the broker backend endpoint."""

    KVS_FILENAME = "key_value_store.ipc"
    """str: Name of IPC file handle used for the key value store endpoint."""

    @classmethod
    def _makeSurePathExists(cls):
        """Verify that the path holding the IPC file handles exists, or if it doesn't, create it."""
        if not isdir(cls.ROOT_DIR):
            makedirs(cls.ROOT_DIR, exist_ok=True)

        instance_path = join(cls.ROOT_DIR, Config.socket.instance_id)
        if not isdir(instance_path):
            makedirs(instance_path, exist_ok=True)

    @classmethod
    def getBrokerProducerQueue(cls, connection: EndpointSpecification.Connection) -> str:
        """Returns the endpoint address for job submission."""
        cls._makeSurePathExists()
        path = join(cls.ROOT_DIR, Config.socket.instance_id, cls.BROKER_PRODUCER_FILENAME)
        return f"{cls.ENDPOINT_PREFIX}://{path}"

    @classmethod
    def getBrokerConsumerQueue(cls, connection: EndpointSpecification.Connection) -> str:
        """Returns the endpoint address for processed job consumption."""
        cls._makeSurePathExists()
        path = join(cls.ROOT_DIR, Config.socket.instance_id, cls.BROKER_CONSUMER_FILENAME)
        return f"{cls.ENDPOINT_PREFIX}://{path}"

    @classmethod
    def getBrokerBackend(cls, connection: EndpointSpecification.Connection) -> str:
        """Returns the broker backend endpoint string."""
        cls._makeSurePathExists()
        path = join(cls.ROOT_DIR, Config.socket.instance_id, cls.BROKER_BACKEND_FILENAME)
        return f"{cls.ENDPOINT_PREFIX}://{path}"

    @classmethod
    def getKeyValueStore(cls, connection: EndpointSpecification.Connection) -> str:
        """Returns the key value store endpoint string."""
        cls._makeSurePathExists()
        path = join(cls.ROOT_DIR, Config.socket.instance_id, cls.KVS_FILENAME)
        return f"{cls.ENDPOINT_PREFIX}://{path}"