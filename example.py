"""
Initialize the `strmbrkr` interchange service (a.k.a. key-value store).
"""
from strmbrkr import Job, KeyValueStore, QueueManager, WorkerManager

worker_manager = WorkerManager()
queue_manager = QueueManager()

"""
Define some work that needs to be done.
"""
from random import randrange
from time import sleep

def sleepAndReturn(how_long: float, throw: Exception = None):
    """Example work method."""
    sleep(how_long)

    if throw is not None:
        raise throw

    KeyValueStore.appendValue("sleep_list", how_long)
    sleep_list = KeyValueStore.getValue("sleep_list")
    KeyValueStore.setValue("sleep_sum", sum(sleep_list))

    return how_long

jobs = [Job(sleepAndReturn, [randrange(3, 10), ]) for _ in range(10)]

"""
> Note:
>
> The worker processes must be started _after_ the work that's being done has been defined.
> Otherwise, the workers will not be able to resolve the reference to the function pointer.
"""
worker_manager.startWorkers()

"""
Record the time for performance reporting.
"""
from time import time
_start = time()

"""
Enqueue the jobs, wait for the jobs to process, and then report results.
"""
queue_manager.queueJobs(*jobs)
queue_manager.blockUntilProcessed(timeout = 60)
print(f"Completed processing in {time() - _start} seconds")
for result in queue_manager.getResults():
    print(f"Job {result.id} returned {result.status}: {result.retval}")
print(f"Total time processed: {KeyValueStore.getValue('sleep_sum')}")

"""In addition to logging messages from the `strmbrkr` package, the resultant report should look something like this:

Completed processing in 18.03610348701477 seconds
Job 2 returned Status.PROCESSED: 3
Job 1 returned Status.PROCESSED: 4
Job 3 returned Status.PROCESSED: 5
Job 4 returned Status.PROCESSED: 4
Job 0 returned Status.PROCESSED: 8
Job 6 returned Status.PROCESSED: 3
Job 7 returned Status.PROCESSED: 5
Job 5 returned Status.PROCESSED: 9
Job 9 returned Status.PROCESSED: 5
Job 8 returned Status.PROCESSED: 9
Total time processed: 55
"""

"""
Tear down `strmbrkr` infrastructure.
"""
worker_manager.stopWorkers()
queue_manager.stopHandling()
KeyValueStore.stopServerProcess()