from __future__ import annotations
# Standard Library Imports
from collections import defaultdict, deque, namedtuple
from multiprocessing import Process, Lock
from os import kill, getpid
from pickle import loads, dumps
from signal import SIGINT
from threading import Thread, Event as ThreadEvent
from time import time
# Third Party Imports
import zmq
# Package Imports
from .config import Config
from .logging import getLogger
from .endpoints import EndpointSpecification
from .key_value_store import KeyValueStore


def _workerNameGenerator():
        """Generator for creating unique identifiers of :class:`.Worker`s."""
        counter = 0
        while True:
            yield f"Worker-{counter}"
            counter += 1


class Worker(Process):
    """Encapsulation of worker related functionality."""

    READY = b"READY"
    """bytes: Message sent by worker when it is ready to receive jobs."""

    ProcessingStatus = namedtuple("ProcessingStatus", ("client", "job", "time_started"))
    """namedtuple: Encapsulation of data recorded when a :class:`.Worker` starts or completes processing a job."""
    
    name_generator = _workerNameGenerator()

    def __init__(self, processing_lock, daemonic: bool = False):
        """Initialize a :class:`.Worker` instance.

        Args:
            processing_lock (multiprocessing.Lock): ``multiprocessing.Lock`` used to indicate whether this
                :class:`.Worker` is currently processing.
            daemonic (bool): Flag indicating whether this :class:`.Worker` should be daemonic.
        """
        super().__init__()
        self.processing_lock = processing_lock
        self._name = next(Worker.name_generator)
        self.daemon = daemonic
        self._target = self.workLoop
        self._args = (self.processing_lock, )

    _id_sep = "::"
    """str: Separator used in :meth:`Worker.identity`."""

    @property
    def identity(self):
        """str: Combination of this :class:`.Worker`'s name and PID.

        Raises:
            ValueError: If :class:`.Worker` has not been started yet, since the PID won't be available until it has.
        """
        if self.is_alive():
            return f"{self.name}{self._id_sep}{self.pid}"
        else:
            raise ValueError("Worker identity not available until after start is called.")

    @classmethod
    def parseIdentity(cls, identity):
        """Parse the given `identity` into useful information.

        Args:
            identity (str): Instance returned by :meth:`.Worker.identity`.

        Returns:
            tuple[str, int]:
                - (str): Corresponds to :attr:`.Worker.name`.
                - (int): Corresponds to :attr:`.Worker.pid`.
        """
        name, pid = identity.split(cls._id_sep)
        return name, int(pid)

    def workLoop(self, processing_lock: Lock):
        """Continuously poll worker broker for jobs that need to be processed.

        Args:
            processing_lock (multiprocessing.Lock): Lock that will need to be acquired to process jobs.
        """
        try:
            socket = zmq.Context().socket(zmq.REQ)
            socket.identity = self.identity.encode("ascii")
            socket.connect(EndpointSpecification.getBrokerBackend(EndpointSpecification.Connection.CONNECT))

            # Tell broker we're ready for work
            socket.send(self.READY)

            while True:
                event_mask = socket.poll()
                if event_mask:
                    with processing_lock:
                        address, _, serialized = socket.recv_multipart()
                        job = loads(serialized)
                        job.process()
                        socket.send_multipart([address, b"", dumps(job)])

        except KeyboardInterrupt:
            print("%s - Received KeyboardInterrupt. Terminating...", self.identity)

    def stop(self, no_wait=False):
        """Halt the execution of :meth:`.Worker.workLoop()`.

        Args:
            no_wait (bool, optional): Flag indicating whether to wait for worker process to finish its current job
                before terminating it.
        """
        if getpid() != self._parent_pid:
            raise RuntimeError("Stop must be called by parent process!")
        if no_wait:
            self.terminate()
            self.join()
        else:
            acquired = self.processing_lock.acquire(timeout=WorkerManager.DEFAULT_WATCHDOG_TERMINATE_AFTER)
            if not acquired:
                getLogger().warning("Failed to acquire processing lock for %s. Terminating anyway!", self._identity)
            self.terminate()
            self.join()
            if acquired:
                self.processing_lock.release()

    @property
    def is_processing(self):
        """``bool``: Boolean indicating whether this :class:`.Worker` is still processing."""
        if not self.processing_lock.acquire(block=False):
            return True
        else:
            self.processing_lock.release()

        return False


class WorkerManager:
    """Class for managing worker processes."""

    DEFAULT_PROC_COUNT = Config.worker.proc_count
    """``int``: Default number of worker processes spun up by an instance of :class;`.WorkerManager`."""

    DEFAULT_WATCHDOG_TERMINATE_AFTER = Config.worker.watchdog_terminate_after
    """``int``: The default number of seconds a worker can spend processing a single job before being terminated."""

    def __init__(
        self,
        proc_count: int | None = None,
        daemonic: bool = True,
        watchdog_terminate_after: int | None = DEFAULT_WATCHDOG_TERMINATE_AFTER
    ):
        """Instantiate a :class:`.WorkerManager` object.

        Args:
            proc_count (``int``|``NoneType``): Number of worker processes to spin up.
            daemonic (``bool``): Flag indicating whether worker threads should be daemonic or not. If
                worker threads are not flagged as daemonic, then it is up to the user to call
                :meth:`.stopWorkers()` to clean up the workers before the program ends.
            watchdog_terminate_after (``int``|``None``): Number of seconds a worker can spend processing a single job before
                being terminated. If set to ``None``, then a worker will be allowed to process a single job indefinitely.
        """
        self._logger = getLogger()
        self._worker_processes = {}

        if proc_count is None:
            proc_count = self.DEFAULT_PROC_COUNT

        if not isinstance(proc_count, int):
            err = f"proc_count must be an int, not {type(proc_count)}"
            raise TypeError(err)

        if proc_count < 1:
            err = f"Unclear proc_count specified: {proc_count}"
            raise ValueError(err)

        for _ in range(proc_count):
            new_worker = Worker(Lock(), daemonic=daemonic)
            self._worker_processes[new_worker.name] = new_worker

        self._processing = {}
        self._watchdog_terminate_after = watchdog_terminate_after
        self._broker_thread = Thread(target=self.brokerLoop, daemon=True)
        self._started = ThreadEvent()

    @property
    def started(self):
        """bool: Indication of whether the :class:`.Worker`s have been started."""
        return self._started.is_set()

    @property
    def worker_count(self):
        """int: Number of :class:`.Worker`s managed by this :class:`.WorkerManager`."""
        return len(self._worker_processes)

    def startWorkers(self):
        """Start worker processes and broker thread."""
        self._started.set()
        self._broker_thread.start()
        for worker in self._worker_processes.values():
            worker.start()

    def stopWorkers(self, no_wait=False):
        """Stop worker processes.

        Args:
            no_wait (``bool``, optional): Flag indicating whether to wait for worker processes to
                finish their current processing before terminating them.
        """
        if self.started:
            for worker in self._worker_processes.values():
                worker.stop(no_wait=no_wait)
            self._started.clear()
            self._broker_thread.join()

    @property
    def is_processing(self):
        """``bool``: Boolean indicating whether one or more workers are still processing."""
        return any(worker.is_processing for worker in self._worker_processes.values())

    def brokerLoop(self):
        self._logger.debug("Starting broker process...")
        # Prepare context and sockets
        context = zmq.Context.instance()
        producer = context.socket(zmq.ROUTER)
        producer.bind(EndpointSpecification.getBrokerProducerQueue(EndpointSpecification.Connection.BIND))
        consumer = context.socket(zmq.ROUTER)
        consumer.bind(EndpointSpecification.getBrokerConsumerQueue(EndpointSpecification.Connection.BIND))
        backend = context.socket(zmq.ROUTER)
        backend.bind(EndpointSpecification.getBrokerBackend(EndpointSpecification.Connection.BIND))

        processed_queue = defaultdict(deque)
        consumer_waiting = dict()

        backend_ready = False
        workers = deque()
        poller = zmq.Poller()
        # Always poll for available workers and consumers
        poller.register(backend, zmq.POLLIN)
        poller.register(consumer, zmq.POLLIN)

        while self.started:
            sockets = dict(poller.poll(1000))

            if backend in sockets:
                # Handle worker activity on the backend
                request = backend.recv_multipart()
                worker, empty, data = request[:3]
                self._processing[worker] = Worker.ProcessingStatus(None, None, time())
                workers.append(worker)
                if workers and not backend_ready:
                    # Poll for clients now that a worker is available and backend was not ready
                    poller.register(producer, zmq.POLLIN)
                    backend_ready = True
                if data != Worker.READY and len(request) > 3:
                    # If client reply, send rest back to frontend
                    empty, reply = request[3:]
                    KeyValueStore.updatePipelineProcessed(loads(reply).id)
                    processed_queue[data].append(reply)

            if producer in sockets:
                # Get next client request, route to last-used worker
                client, request = producer.recv_multipart()

                worker = workers.popleft()
                backend.send_multipart([worker, b"", client, b"", request])
                job = loads(request)
                self._processing[worker] = Worker.ProcessingStatus(client, job, time())
                KeyValueStore.updatePipelineDelegated(job.id)
                if not workers:
                    # Don't poll clients if no workers are available and set backend_ready flag to false
                    poller.unregister(producer)
                    backend_ready = False

            if consumer in sockets:
                client, empty, request = consumer.recv_multipart()
                consumer_waiting[client] = True

            for client, waiting in consumer_waiting.items():
                if waiting and processed_queue[client]:
                    consumer.send_multipart([client, b"", processed_queue[client].popleft()])
                    consumer_waiting[client] = False

            killed = list()
            now = time()
            self._logger.debug("Now: %f", now)
            for worker, status in self._processing.items():
                process_duration = now - status.time_started
                if status.job:
                    self._logger.debug("Worker '%s' processing job '%s' for %.2f seconds.", str(worker), status.job.id, process_duration)
                    if self._watchdog_terminate_after is not None:
                        if process_duration > self._watchdog_terminate_after:
                            worker_name, worker_pid = Worker.parseIdentity(worker.decode('ascii'))
                            kill(worker_pid, SIGINT)
                            del self._worker_processes[worker_name]
                            killed.append(worker)
                            msg = f"Terminating worker '{worker}' because it's been processing job '{status.job.id}' for "
                            msg += f"{self._watchdog_terminate_after} seconds."
                            self._logger.error(msg)
                            status.job.error = RuntimeError(msg)
                            processed_queue[client].append(dumps(status.job))
                else:
                    self._logger.debug("Worker '%s' idle for %.2f seconds.", str(worker), process_duration)

            for worker in killed:
                del self._processing[worker]

        producer.close(linger=0)
        consumer.close(linger=0)
        backend.close(linger=0)
