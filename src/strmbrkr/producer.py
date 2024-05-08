from __future__ import annotations
# Standard Imports
from pickle import dumps, loads
from queue import Queue
from sys import maxsize
from threading import Thread, Event
from time import sleep
from traceback import format_exc
from uuid import uuid4
# Third Party Imports
import zmq
# Package Imports
from .logging import getLogger
from .endpoints import EndpointSpecification
from .job import Job
from .key_value_store import KeyValueStore


class JobTimeoutError(Exception):
    """Exception indicating jobs haven't completed within a given timeout."""


class QueueManager:
    """Class for managing queuing of new jobs and handling of completed jobs."""

    _BLOCK_INTERVAL = 1
    """``float``: How many seconds to wait between checking for job completion while blocking."""

    def __init__(self, processed_callback: callable = None):
        """Initialize a :class:`.QueueManager` instance.

        Args:
            processed_callback (callable): Callable that accepts one argument that will be a processed :class:`.Job`
                instance.
        """
        self._logger = getLogger()
        self._unique_id = str(uuid4())

        context = zmq.Context.instance()
        self._producer = context.socket(zmq.DEALER)
        self._producer.identity = self._unique_id.encode("utf-8")
        self._producer.connect(EndpointSpecification.getBrokerProducerQueue(EndpointSpecification.Connection.CONNECT))

        self._queued_job_ids = set()

        self._results = Queue()
        self._callback = processed_callback

        self._job_handler_exception = None
        self._job_handler_traceback = None

        self._stop_handling = Event()
        self._processed_job_handler = Thread(target=self._handleProcessedJobs, daemon=True)
        self._processed_job_handler.start()

    def queueJobs(self, *args):
        """Add jobs to the queue.

        Args:
            *args (``iterable``): Variable length list of :class:`.Job` objects to be queued.
        """
        for job in args:
            if not isinstance(job, Job):
                err = f"Cannot queue objects of '{type(job)}'"
                raise TypeError(err)

            self._logger.debug("Queuing job %s", job.id)
            self._queued_job_ids.add(job.id)
            KeyValueStore.updatePipelineQueued(job.id)
            self._producer.send(dumps(job))

    def _handleProcessedJobs(self):
        """Handle processed jobs returned from workers.

        While there are queued jobs, this method continuously requests processed jobs on the "consumer" socket. This method
        is automatically started in another ``threading.Thread`` during the construction of a :class:`.QueueManager` .

        If no ``processed_callback`` is given to this :class:`.QueueManager` 's constructor, then
        handling processed jobs defaults to logging the job's reception and putting it on a
        ``Queue`` . These are retrievable via :meth:`.getResults()` .

        If ``processed_callback`` *is* given to the constructor, then handling will consist of
        calling ``processed_callback`` with the processed :class:`.Job` as its single argument.
        """
        context = zmq.Context.instance()
        consumer_socket = context.socket(zmq.REQ)
        consumer_socket.identity = self._unique_id.encode("utf-8")
        consumer_socket.connect(EndpointSpecification.getBrokerConsumerQueue(EndpointSpecification.Connection.CONNECT))
        while not self._stop_handling.is_set():
            if not self._queued_job_ids:
                sleep(1)
                continue
            
            consumer_socket.send(b"READY")
            ret = consumer_socket.recv()
            try:
                job = loads(ret)

                if self._callback is None:
                    msg = f"Job {job.id} returned {job.retval} with status code {job.status}"
                    if job.status == Job.Status.PROCESSED:
                        self._logger.debug(msg)
                    else:
                        self._logger.error(msg + f":{job.error}")
                    self._results.put(job)
                else:
                    self._callback(job)

            except Exception as error:  # pylint: disable=broad-except
                self._job_handler_exception = error
                self._job_handler_traceback = format_exc()

            finally:
                self._queued_job_ids.remove(job.id)
                KeyValueStore.updatePipelineReturned(job.id)

    @property
    def queued_jobs_processed(self):
        """``bool``: Boolean indicating whether all queued jobs have been processed and handled."""
        return len(self._queued_job_ids) == 0

    @property
    def queued_job_ids(self):
        """``set``: Set of job IDs that have been queued, but not processed."""
        return self._queued_job_ids

    def blockUntilProcessed(self, timeout=None, dump_on_timeout=True):
        """Block until all queued jobs have been processed and handled.

        Args:
            timeout (``float``, optional): How long to wait for jobs to be processed and handled.
                Defaults to ``None``, which will wait indefinitely for jobs to process and be
                handled.
            dump_on_timeout (``bool``, optional): Flag indicating that the contents of the key value store should be dumped
                if `timeout` is not ``None`` and it is reached before jobs have finished processing.
        """
        interval = self._BLOCK_INTERVAL
        if timeout is not None:
            if timeout < self._BLOCK_INTERVAL:
                interval = timeout
        else:
            timeout = maxsize

        waited = 0.0
        while not self.queued_jobs_processed:
            sleep(interval)
            waited += interval

            if self._job_handler_exception is not None:
                self._logger.error("Error occurred in job handler thread: \n%s", self._job_handler_traceback)
                raise self._job_handler_exception

            if waited >= timeout:
                if not self.queued_jobs_processed:
                    if dump_on_timeout:
                        KeyValueStore.dump()
                    raise JobTimeoutError(f"Reached timeout with {len(self._queued_job_ids)} jobs left to process.")

    def getResults(self):
        """Retrieve processed :class:`.Job` s for post-processing.

        Note:
            It is the user's responsibility to utilize :meth:`.blockUntilProcessed()` or
            :attr:`.queued_jobs_processed` to wait for currently queued jobs to complete. Calling
            :meth:`.getResults()`  before all jobs have been processed could result in jobs being
            left on the :attr:`._results` queue, which could lead to errors in subsequent batches
            of processing.
        """
        if self._job_handler_exception is not None:
            self._logger.error("Error occurred in job handler thread: \n%s", self._job_handler_traceback)
            raise self._job_handler_exception

        ret = []
        while not self._results.empty():
            ret.append(self._results.get())

        return ret

    def stopHandling(self):
        """Set flag high to terminate the processed job thread and join the thread."""
        self._stop_handling.set()
        self._processed_job_handler.join()
