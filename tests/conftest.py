# Third Party Imports
import pytest
# Package Imports
from strmbrkr.key_value_store.job_status_transaction import InvalidStatusUpdate
from strmbrkr.key_value_store.key_value_store import KeyValueStore


def pytest_addoption(parser):
    """Add command line options."""
    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")


def pytest_configure(config):
    """Configure pytest options without an .ini file."""
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    """Collect pytest modifiers."""
    if config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


@pytest.fixture(autouse=True, scope="session")
def ignorePipelineStatusUpdateErrors():
    KeyValueStore.setIgnoreTransactExc((InvalidStatusUpdate, ))
