# STRMBRKR

## **S**imultaneous **T**ask **R**egistrar and **M**essage **Br**o**k**e**r**

_The hammer for all of your parallel processing nails._

This package abstracts `zeroMQ`'s [Load Balancing Message Broker](https://zguide.zeromq.org/docs/chapter3/#A-Load-Balancing-Message-Broker) design pattern to facilitate the brokering of processor-intensive workloads across parallel workers.
This package also provides a `KeyValueStore` implementation inspired by [Redis](https://docs.redis.com/latest/index.html) and built on `zeroMQ`'s `REQ`/`REP` socket pattern that facilitates inter-process communication.

## Installation

To install the `strmbrkr` package, you'll need Python and `pip` installed.
There are a variety of ways to do this, such as using [Miniconda](https://docs.conda.io/en/latest/miniconda.html), so it's left to the user to utilize their preferred method.

### Installing in Development Mode

This method is recommended if you plan on making updates to the source code and don't want to re-install that package with each change.
Clone this repository and install using `pip`:

```
$ git clone git@github.com:kdavid13/strmbrkr.git
$ cd ./strmbrkr
$ pip install -e .
```

## Usage

Here is an example script (available as `example.py`) that utilizes the `strmbrkr` package.
It is standalone and should run on any system with the `strmbrkr` package properly installed.
For more examples of `strmbrkr` edge cases like error handling and watchdog timers, peruse the `tests/` directory.

```python
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
```

## Configuration

When utilizing `strmbrkr` library components, you may find it useful to change their default behaviors.
`strmbrkr` will look two places for configuration options: environment variables and a `strmbrkr.env` file in the current working directory.
The configuration options available are as follows:

 - `STRMBRKR_LOGGING_LEVEL` (`str`): Level at which the STRMBRKR module logger will emit messages. Default: `INFO`.
 - `STRMBRKR_CONFIG_LOG_LEVEL` (`str`): Level at which to log the contents of the configuration. Default: `DEBUG`.
 - `STRMBRKR_WORKER_PROC_COUNT` (`int`): Default number of worker processes spun up by an instance of `WorkerManager`. Default: `4`.
 - `STRMBRKR_WORKER_WATCHDOG_TERMINATE_AFTER` (`int`): The default number of seconds a worker can spend processing a single job before being terminated. Default: `15`.
 - `STRMBRKR_KVS_CLIENT_SOCKET_TIMEOUT` (`int`): Connection timeout (in milliseconds) of the `KeyValueStore` client. Default: `500`.
 - `STRMBRKR_KVS_CLIENT_SOCKET_ATTEMPTS` (`int`): How many times the `KeyValueStore` client will attempt to connect to the server before failing. Default: `1`.
 - `STRMBRKR_KVS_DUMP_PIPELINE_STATUS_REPORTS` (`bool`): Flag that indicates whether to save pipeline status reports for debugging. Default: `False`.
 - `STRMBRKR_SOCKET_PROTOCOL` (`str`): The type of socket protocol that strmbrkr will use. Default: `tcp`.
 - `STRMBRKR_INSTANCE_ID` (`str`): A unique identifier for the `strmbrkr` 'instance' using this configuration. Default: the process ID of Python interpreter instance running what is colloquially known as the 'control thread'.
 
See documentation in the `strmbrkr.config` submodule for more information.
