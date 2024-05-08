# Standard Library Imports
from time import time
from random import randrange, seed
from subprocess import Popen
# Third Party Imports
import pytest
# Package Imports
from strmbrkr import Job, QueueManager, WorkerManager
# Testing Imports
from . import sleepAndReturn


@pytest.mark.slow
def test_parallelAPI():
    seed(1234)
    _start = time()
    worker_manager = WorkerManager()
    worker_manager.startWorkers()

    queue_manager = QueueManager()
    
    jobs = [Job(sleepAndReturn, [randrange(3, 10), ]) for _ in range(10)]

    queue_manager.queueJobs(*jobs)
    queue_manager.blockUntilProcessed(timeout=60)
    print(f"Completed processing in {time() - _start} seconds")
    for result in queue_manager.getResults():
        print(f"Job {result.id} returned {result.status}: {result.retval}")

    worker_manager.stopWorkers()
    queue_manager.stopHandling()


@pytest.mark.slow
def test_multipleQueueManagers():
    seed(1234)
    _start = time()
    worker_manager = WorkerManager()
    worker_manager.startWorkers()

    queue_manager_0 = QueueManager()
    queue_manager_1 = QueueManager()
    
    for _ in range(10):
        queue_manager_0.queueJobs(
            Job(sleepAndReturn, [randrange(3, 7)])
        )
        queue_manager_1.queueJobs(
            Job(sleepAndReturn, [randrange(3, 7)])
        )

    queue_manager_0.blockUntilProcessed(timeout = 60)
    for result in queue_manager_0.getResults():
        print(f"Job {result.id} returned {result.status}: {result.retval}")

    queue_manager_1.blockUntilProcessed(timeout = 60)
    for result in queue_manager_1.getResults():
        print(f"Job {result.id} returned {result.status}: {result.retval}")

    worker_manager.stopWorkers()
    queue_manager_0.stopHandling()
    queue_manager_1.stopHandling()
