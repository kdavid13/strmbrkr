"""Validate the functionality of the :class:`.QueueManager` class."""
# Standard Library Imports
from pickle import dumps
from time import sleep
# Third Party Imports
import pytest
import zmq
# Package Imports
from strmbrkr.endpoints import EndpointSpecification
from strmbrkr import Job, QueueManager
# Testing Imports
from . import sleepAndReturn


@pytest.fixture
def broker_producer():
    """Return ``zmq.Socket`` bound to the broker 'producer' endpoint"""
    context = zmq.Context.instance()
    producer = context.socket(zmq.ROUTER)
    producer.bind(EndpointSpecification.getBrokerProducerQueue(EndpointSpecification.Connection.BIND))

    yield producer

    producer.close(linger=0)


@pytest.fixture
def broker_consumer():
    """Return ``zmq.Socket`` bound to the broker 'consumer' endpoint"""
    context = zmq.Context.instance()
    consumer = context.socket(zmq.ROUTER)
    consumer.bind(EndpointSpecification.getBrokerConsumerQueue(EndpointSpecification.Connection.BIND))

    yield consumer

    consumer.close(linger=0)


def test_init():
    mgr = QueueManager()

    assert mgr.queued_jobs_processed is True
    assert not mgr.queued_job_ids
    assert not mgr.getResults()

    mgr.stopHandling()


def test_queueJob(broker_producer, broker_consumer):
    mgr = QueueManager()
    job = Job(sleepAndReturn, args=[3])
    mgr.queueJobs(job)

    assert mgr.queued_jobs_processed is False
    assert len(mgr.queued_job_ids) == 1

    with pytest.raises(Exception):
        mgr.blockUntilProcessed(timeout = 3)

    with pytest.raises(Exception):
        mgr.blockUntilProcessed(timeout = (QueueManager._BLOCK_INTERVAL / 2))

    client, request = broker_producer.recv_multipart()
    job.process()
    broker_consumer.send_multipart([client, b"", dumps(job)])

    sleep(1)

    assert mgr.queued_jobs_processed
    assert not mgr.queued_job_ids
    assert mgr.getResults()

    mgr.stopHandling()


def test_stopHandling():
    mgr = QueueManager()
    mgr.stopHandling()
    assert not mgr._processed_job_handler.is_alive()


def test_queueNotJob():
    mgr = QueueManager()

    with pytest.raises(TypeError):
        mgr.queueJobs(print)

    mgr.stopHandling()


def test_queueJobException(broker_producer, broker_consumer):
    mgr = QueueManager()
    job = Job(sleepAndReturn, args=[3])
    mgr.queueJobs(job)

    assert mgr.queued_jobs_processed is False
    assert len(mgr.queued_job_ids) == 1

    with pytest.raises(Exception):
        mgr.blockUntilProcessed(timeout = 3)

    client, request = broker_producer.recv_multipart()
    job.error = repr(Exception("Forced error"))
    client, empty, request = broker_consumer.recv_multipart()
    broker_consumer.send_multipart([client, b"", dumps(job)])

    sleep(1)

    assert mgr.queued_jobs_processed
    assert not mgr.queued_job_ids
    assert mgr.getResults()

    mgr.stopHandling()


def test_jobCallback(broker_producer, broker_consumer):
    def jobCallback(job):
        print(f"Handling job '{job.id}' which returned '{job.retval}'")
    mgr = QueueManager(processed_callback=jobCallback)
    job = Job(sleepAndReturn, args=[3])
    print(f"Queuing job {job.id}")
    mgr.queueJobs(job)

    assert mgr.queued_jobs_processed is False
    assert len(mgr.queued_job_ids) == 1

    with pytest.raises(Exception):
        mgr.blockUntilProcessed(timeout = 3)

    client, request = broker_producer.recv_multipart()
    job.process()
    client, empty, request = broker_consumer.recv_multipart()
    broker_consumer.send_multipart([client, b"", dumps(job)])

    sleep(1)

    assert mgr.queued_jobs_processed, f"jobs still queued: {mgr.queued_job_ids}"
    assert not mgr.queued_job_ids
    assert not mgr.getResults()

    mgr.stopHandling()


def test_jobCallbackError(broker_producer, broker_consumer):
    def jobCallbackError(job):
        sleep(2)
        raise Exception("forced error")
    
    mgr = QueueManager(processed_callback=jobCallbackError)
    job = Job(sleepAndReturn, args=[3])
    mgr.queueJobs(job)

    assert mgr.queued_jobs_processed is False
    assert len(mgr.queued_job_ids) == 1

    client, request = broker_producer.recv_multipart()
    job.retval = 3
    client, empty, request = broker_consumer.recv_multipart()
    broker_consumer.send_multipart([client, b"", dumps(job)])

    with pytest.raises(Exception):
        mgr.blockUntilProcessed()

    sleep(1)

    assert mgr.queued_jobs_processed
    assert not mgr.queued_job_ids
    with pytest.raises(Exception):
        _ = mgr.getResults()

    mgr.stopHandling()
