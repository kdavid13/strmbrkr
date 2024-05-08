# Package Imports
from strmbrkr import Job
# Testing Imports
from . import sleepAndReturn


def test_init():
    job = Job(
        sleepAndReturn,
        args=[1]
    )
    assert isinstance(job.id, str)
    assert job.status == Job.Status.UNPROCESSED


def test_jobIdGeneration():
    first_job = Job(
        sleepAndReturn,
        args=[1]
    )
    second_job = Job(
        sleepAndReturn,
        args=[1]
    )
    assert first_job.id != second_job.id


def test_process():
    args = [1]
    job = Job(
        sleepAndReturn,
        args=args
    )
    job.process()
    assert job.status == Job.Status.PROCESSED
    assert job.retval == args[0]


def test_error():
    args = [1]
    kwargs = {"throw": RuntimeError("Expected error.")}
    job = Job(
        sleepAndReturn,
        args=args,
        kwargs=kwargs
    )
    job.process()
    assert job.status == Job.Status.FAILED
    assert job.error is not None
