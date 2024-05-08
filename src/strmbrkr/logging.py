# Standard Library Imports
import logging
from logging.config import dictConfig
# Package Imports
from .config import Config, LOG_LEVEL_MAPPING


PACKAGE_NAME = "strmbrkr"
"""str: Name of this package."""


DEFAULT_LEVEL = Config.logging.level
"""int: Lowest logging severity that will be emitted to the configured logging destination."""


dictConfig({
    "version": 1,
    "formatters": {
        "default": {
            "format": "%(asctime)s - %(process)d - %(module)s - %(levelname)s - %(message)s"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": DEFAULT_LEVEL,
            "formatter": "default",
            "stream": "ext://sys.stdout"
        }
    },
    "loggers": {
        PACKAGE_NAME: {
            "level": DEFAULT_LEVEL,
            "handlers": ["console"]
        }
    }
})
"""Configure this package's logger object."""


def getLogger():
    """Return a reference to this package's logger object."""
    return logging.getLogger(PACKAGE_NAME)


getLogger().log(LOG_LEVEL_MAPPING[Config.logging.config_log_level], "srmbrkr configuration: %s", repr(Config))
