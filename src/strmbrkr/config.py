# Standard Library Imports
from copy import copy
from dataclasses import asdict, dataclass, InitVar
from logging import _nameToLevel
from os import environ, getpid
from os.path import isfile
from typing import Any, ClassVar


LOG_LEVEL_MAPPING = copy(_nameToLevel)
"""dict: Keys are names of logging levels and values are their integer counterparts."""


def readEnvFile(filename: str) -> dict:
    """Read a '.env' file and return the contents as a dictionary.

    Args:
        filename (str): Name of the '.env' file to read.

    Returns:
        dict: The contents of the specified '.env' file stored in a dictionary.
    """
    file_env = {}
    with open(filename, 'r') as env_file:
        line = env_file.readline()
        while line:
            line = line.strip()
            key, value = line.split('=')
            value = value.strip('"')
            file_env[key] = value

            line = env_file.readline()
    return file_env


def getHybridEnv(env_file_name: str = "strmbrkr.env"):
    """Return a dictionary comprised of the OS environment variables updated with the variables defined in `env_file_name`.

    Args:
        env_file_name (str): Path to environment file.

    Returns:
        dict: Dictionary comprised of the OS environment variables updated with the variables defined in `env_file_name`.
    """
    _env = environ.copy()
    if isfile(env_file_name):
        _env.update(readEnvFile(env_file_name))
    return _env


class ConfigError(ValueError):
    """Exception raised when config value provided is invalid."""

    def __init__(self, option: str, value: Any):
        err = f"'{value}' not a valid '{option}' setting."
        super().__init__(err)


@dataclass
class _LoggingConfig:
    """Encapsulates configurable options associated with logging."""

    level_env_name: ClassVar[str] = "STRMBRKR_LOGGING_LEVEL"
    level: str = "INFO"
    """``str``: Level at which the strmbrkr module logger will emit messages."""

    config_log_level_env_name: ClassVar[str] = "STRMBRKR_CONFIG_LOG_LEVEL"
    config_log_level: str = "DEBUG"
    """``str``: Level at which to log the contents of the configuration."""

    env: InitVar[dict] = None
    """dict: The root :class:`._Config` class will pass in the results of :meth:`.getHybridEnv()`."""

    def __post_init__(self, env):
        env = getHybridEnv()
        env_level = env.get(self.level_env_name)
        if env_level is not None:
            self.level = env_level
            if self.level not in LOG_LEVEL_MAPPING:
                raise ConfigError(self.level_env_name, self.level)

        env_conf_level = env.get(self.config_log_level_env_name)
        if env_conf_level is not None:
            self.config_log_level = env_conf_level
            if self.config_log_level not in LOG_LEVEL_MAPPING:
                raise ConfigError(self.config_log_level_env_name, self.config_log_level)


@dataclass
class _WorkerConfig:
    """Encapsulates configurable options associated with the :class:`.WorkerManager` component."""

    proc_count_env_name: ClassVar[str] = "STRMBRKR_WORKER_PROC_COUNT"
    proc_count: int = 4
    """``int``: Default number of worker processes spun up by an instance of :class:`.WorkerManager`."""

    watchdog_terminate_after_env_name: ClassVar[str] = "STRMBRKR_WORKER_WATCHDOG_TERMINATE_AFTER"
    watchdog_terminate_after: int = 15
    """``int``: The default number of seconds a worker can spend processing a single job before being terminated."""

    env: InitVar[dict] = None
    """dict: The root :class:`._Config` class will pass in the results of :meth:`.getHybridEnv()`."""

    def __post_init__(self, env):
        env_proc_count = env.get(self.proc_count_env_name)
        if env_proc_count is not None:
            self.proc_count = int(env_proc_count)

        env_watchdog = env.get(self.watchdog_terminate_after_env_name)
        if env_watchdog is not None:
            self.watchdog_terminate_after = int(env_watchdog)


@dataclass
class _KeyValueStoreConfig:
    """Encapsulates configurable options associated with the :class:`.KeyValueStore` component."""

    client_socket_timeout_env_name: ClassVar[str] = "STRMBRKR_KVS_CLIENT_SOCKET_TIMEOUT"
    client_socket_timeout: int = 500
    """``int``: Connection timeout (in milliseconds) of the :class:`.KeyValueStore` client."""

    client_socket_attempts_env_name: ClassVar[str] = "STRMBRKR_KVS_CLIENT_SOCKET_ATTEMPTS"
    client_socket_attempts: int = 1
    """``int``: How many times the :class:`.KeyValueStore` client will attempt to connect to the server before failing."""

    dump_pipeline_status_reports_env_name: ClassVar[str] = "STRMBRKR_KVS_DUMP_PIPELINE_STATUS_REPORTS"
    dump_pipeline_status_reports: bool = False
    """``bool``: Flag that indicates whether to save pipeline status reports for debugging."""

    env: InitVar[dict] = None
    """dict: The root :class:`._Config` class will pass in the results of :meth:`.getHybridEnv()`."""

    def __post_init__(self, env):
        env_sock_timeout = env.get(self.client_socket_timeout_env_name)
        if env_sock_timeout is not None:
            self.client_socket_timeout = int(env_sock_timeout)

        env_sock_attempts = env.get(self.client_socket_attempts_env_name)
        if env_sock_attempts is not None:
            self.client_socket_attempts = int(env_sock_attempts)

        env_dump_pipelines = env.get(self.dump_pipeline_status_reports_env_name)
        if env_dump_pipelines is not None:
            self.dump_pipeline_status_reports = bool(env_dump_pipelines)


@dataclass
class _SocketConfig:
    """Encapsulates configurable options for the ZMQ sockets being used."""

    instance_id_name = "STRMBRKR_INSTANCE_ID"

    protocol_env_name = "STRMBRKR_SOCKET_PROTOCOL"
    protocol: str = "tcp"
    """str: The type of socket protocol that strmbrkr will use."""

    env: InitVar[dict] = None
    """dict: The root :class:`._Config` class will pass in the results of :meth:`.getHybridEnv()`."""

    def __post_init__(self, env):
        env_protocol = env.get(self.protocol_env_name)
        if env_protocol is not None:
            self.protocol = env_protocol

        _ = self.instance_id  # initialize instance ID

    @property
    def instance_id(self):
        """A unique identifier for the ``strmbrkr`` 'instance' using this configuration.

        Presumably, when a user imports a ``strmbrkr`` component in a script, and needs to run multiple instances of that script
        (or other scripts that import ``strmbrkr`` components) concurrently, the user would expect that the ``strmbrkr``
        components would not interact across those script instances.

        This ``instance_id`` (along with the logic contained in the :class:`.EndpointSpecification` classes) facilitates that
        functionality and ensures that operating system resources are not shared across different ``strmbrkr`` API instances.
        """
        instance_id = getHybridEnv().get(self.instance_id_name)
        if instance_id is None:
            instance_id = str(getpid())
            environ[self.instance_id_name] = instance_id
        return instance_id


@dataclass
class _Config:
    """Root configuration class."""

    logging: _LoggingConfig = None
    worker: _WorkerConfig = None
    key_value_store: _KeyValueStoreConfig = None
    socket: _SocketConfig = None

    def __post_init__(self):
        env = getHybridEnv()
        self.logging = _LoggingConfig(env=env)
        self.worker = _WorkerConfig(env=env)
        self.key_value_store = _KeyValueStoreConfig(env=env)
        self.socket = _SocketConfig(env=env)

    def __repr__(self):
        _dict = asdict(self)
        _dict["socket"]["instance_id"] = self.socket.instance_id

        return str(_dict)


Config = _Config()
"""Root configuration instance containing all sub-configuration instances."""
