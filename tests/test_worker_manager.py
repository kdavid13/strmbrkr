"""Test the functionality of the :class:`.WorkerManager` class."""
# Standard Library Imports
from pickle import dumps, loads
from random import random
from uuid import uuid4
# Third Party Imports
import pytest
import zmq
# Package Imports
from strmbrkr.endpoints import EndpointSpecification
from strmbrkr import Job, WorkerManager
# Tests Imports
from . import sleep, sleepAndReturn


@pytest.fixture(scope="session")
def socket_identity():
    yield uuid4().hex


@pytest.fixture
def client_producer(socket_identity):
    """Return ``zmq.Socket`` connected to the broker 'producer' endpoint"""
    context = zmq.Context.instance()
    producer = context.socket(zmq.DEALER)
    producer.identity = socket_identity.encode('utf-8')
    producer.connect(EndpointSpecification.getBrokerProducerQueue(EndpointSpecification.Connection.CONNECT))

    yield producer

    producer.close(linger=0)


@pytest.fixture
def client_consumer(socket_identity):
    """Return ``zmq.Socket`` connected to the broker 'consumer' endpoint"""
    context = zmq.Context.instance()
    consumer = context.socket(zmq.REQ)
    consumer.identity = socket_identity.encode('utf-8')
    consumer.connect(EndpointSpecification.getBrokerConsumerQueue(EndpointSpecification.Connection.CONNECT))

    yield consumer

    consumer.close(linger=0)


def test_init():
    """Validate basic functionality of a constructed :class:`.WorkerManager` instance."""
    mgr = WorkerManager()

    assert not mgr.started
    assert not mgr.is_processing


def test_initProcCountStr():
    """Validate that `proc_count` argument is properly type checked."""
    with pytest.raises(TypeError):
        mgr = WorkerManager(proc_count = "five")


def test_initProcCountZero():
    with pytest.raises(ValueError):
        mgr = WorkerManager(proc_count = 0)


def test_startStop():
    """Validate :meth:`WorkerManager.startWorkers()` and :meth:`Worker.stopWorkers()`."""
    mgr = WorkerManager()

    mgr.startWorkers()

    assert mgr.started
    assert not mgr.is_processing

    mgr.stopWorkers()


def test_processJob(client_producer, client_consumer):
    """Validate that a valid :class:`.Job` is processed correctly through a :class:`.WorkerManager`."""
    mgr = WorkerManager()
    mgr.startWorkers()

    job_args = [1]
    job = Job(
        sleepAndReturn,
        args=job_args
    )
    client_producer.send(dumps(job))

    is_processing = mgr.is_processing
    if not is_processing:
        delay = job_args[0] / 2
        sleep(delay)
        assert mgr.is_processing, f"WorkerManager hasn't started processing job after {delay} seconds."

    client_consumer.send(b"READY")
    ret = client_consumer.recv()
    job_reply = loads(ret)

    assert job_reply.status == Job.Status.PROCESSED
    assert job_reply.retval == job_args[0]

    mgr.stopWorkers()


def test_multiJob(client_producer, client_consumer):
    """Validate that :class:`.WorkerManager` will handle jobs queued faster than they can be processed."""
    mgr = WorkerManager()
    mgr.startWorkers()

    queued_jobs = {}
    jobs_args = {}

    for _ in range(mgr.worker_count * 2):
        job_args = [random() * 2]
        job = Job(
            sleepAndReturn,
            args=job_args
        )
        queued_jobs[job.id] = job
        jobs_args[job.id] = job_args
        client_producer.send(dumps(job))

    sleep_count = 5
    while not mgr.is_processing:
        if sleep_count < 1:
            mgr.stopWorkers()
            pytest.fail("WorkerManager hasn't started processing job after 5 seconds.")
        sleep(1)
        sleep_count -= 1

    while queued_jobs:
        client_consumer.send(b"READY")
        ret = client_consumer.recv()
        job_reply = loads(ret)

        assert job_reply.status == Job.Status.PROCESSED
        assert job_reply.retval == jobs_args[job_reply.id][0]
        del queued_jobs[job_reply.id]
        del jobs_args[job_reply.id]

    mgr.stopWorkers()


@pytest.mark.slow
def test_watchdog(client_producer, client_consumer):
    """Validate that :class:`.WorkerManager` watchdog kills jobs that take too long."""
    mgr = WorkerManager()
    mgr.startWorkers()
    orig_worker_count = mgr.worker_count

    job_args = [WorkerManager.DEFAULT_WATCHDOG_TERMINATE_AFTER + 3]
    job = Job(
        sleepAndReturn,
        args=job_args
    )
    client_producer.send(dumps(job))

    client_consumer.send(b"READY")
    ret = client_consumer.recv()
    job_reply = loads(ret)

    assert job_reply.status == Job.Status.FAILED
    assert job_reply.retval is None
    assert mgr.worker_count == (orig_worker_count - 1)

    mgr.stopWorkers()


def test_customWatchdog(client_producer, client_consumer):
    """Validate that :class:`.WorkerManager` watchdog kills jobs that take too long."""
    custom_watchdog = 3
    mgr = WorkerManager(watchdog_terminate_after=custom_watchdog)
    mgr.startWorkers()
    orig_worker_count = mgr.worker_count

    job_args = [custom_watchdog + 3]
    job = Job(
        sleepAndReturn,
        args=job_args
    )
    client_producer.send(dumps(job))

    client_consumer.send(b"READY")
    ret = client_consumer.recv()
    job_reply = loads(ret)

    assert job_reply.status == Job.Status.FAILED
    assert job_reply.retval is None
    assert mgr.worker_count == (orig_worker_count - 1)

    mgr.stopWorkers()
