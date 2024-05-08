"""Test the functionality of the :class:`.Worker` class."""
# Standard Library Imports
from multiprocessing import Lock
from pickle import loads, dumps
# Third Party Imports
import pytest
import zmq
# Package Imports
from strmbrkr import Job
from strmbrkr.worker import EndpointSpecification, Worker
# Tests Imports
from . import sleep, sleepAndReturn


@pytest.fixture
def backend_server():
    """Return a ZMQ socket bound to the :attr:`BROKER_BACKEND` endpoint."""
    context = zmq.Context.instance()
    backend = context.socket(zmq.ROUTER)
    backend.bind(EndpointSpecification.getBrokerBackend(EndpointSpecification.Connection.BIND))
    yield backend
    backend.close(linger=0)


def test_worker_init():
    """Validate basic functionality of a constructed :class:`.Worker` instance."""
    lock = Lock()
    worker = Worker(lock)

    assert worker.processing_lock is lock
    assert worker.daemon is False
    with pytest.raises(ValueError):
        _ = worker.identity
    assert worker.is_processing is False


def test_worker_nameGeneration():
    """Validate worker name generation."""
    first_lock = Lock()
    first_worker = Worker(first_lock)

    second_lock = Lock()
    second_worker = Worker(second_lock)

    assert first_worker.name != second_worker.name


def test_worker_startStop():
    """Validate :meth:`Worker.start()` and :meth:`Worker.stop()`."""
    lock = Lock()
    worker = Worker(lock)
    assert worker.is_alive() is False

    worker.start()
    assert worker.is_alive() is True
    assert isinstance(worker.identity, str)
    name, pid = Worker.parseIdentity(worker.identity)
    assert name == worker.name
    assert isinstance(pid, int)

    worker.stop()
    assert worker.is_alive() is False


def test_worker_ready(backend_server):
    """Validate that a started :class:`.Worker` sends the expected :attr:`.Worker.READY` message to the broker."""
    lock = Lock()
    worker = Worker(lock)
    worker.start()

    event_mask = backend_server.poll(timeout=1000)
    worker.stop()
    if event_mask:
        worker_id, empty, data = backend_server.recv_multipart()
        assert data == Worker.READY
    else:
        pytest.fail("Did not receive Worker.READY message from worker")


def test_workerProcessing(backend_server):
    """Validate functionality of :attr:`.Worker.is_processing` and returned processed :class:`.Job`."""
    lock = Lock()
    worker = Worker(lock)
    worker.start()

    worker_id, empty, data = backend_server.recv_multipart()
    assert data == Worker.READY

    job_args = [1]
    job = Job(
        sleepAndReturn,
        args=job_args
    )

    client_addr = b"client_addr"
    backend_server.send_multipart([worker_id, b"", client_addr, b"", dumps(job)])

    is_processing = worker.is_processing
    if not is_processing:
        delay = job_args[0] / 2
        sleep(delay)
        assert worker.is_processing, f"Worker hasn't started processing job after {delay} seconds."
    
    worker_id, _, reply_addr, _, job_reply = backend_server.recv_multipart()

    job_reply = loads(job_reply)

    assert reply_addr == client_addr
    assert job_reply.status == Job.Status.PROCESSED
    assert job_reply.retval == job_args[0]

    worker.stop()


def test_workerStopNoWait():
    """Validate functionality of :meth:`.Worker.stop()` with the ``no_wait`` flag set high."""
    lock = Lock()
    worker = Worker(lock)
    assert worker.is_alive() is False

    worker.start()
    assert worker.is_alive() is True

    worker.stop(no_wait=True)
    assert worker.is_alive() is False
