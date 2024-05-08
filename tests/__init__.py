# Standard Library Imports
from time import sleep


def sleepAndReturn(how_long: float, throw: Exception = None):
    """Test function that sleeps for `how_long` seconds, and then returns `how_long`.

    Args:
        how_long (float): How many seconds to sleep for.
        throw (Exception, optional): An ``Exception`` to be thrown by this function after the sleep timer expires.

    Returns:
        float: Returns the value specified in `how_long`.

    Raises:
        Exception: If `throw` is populated with an `Exception`, it will be thrown.
    """
    sleep(how_long)

    if throw is not None:
        raise throw

    return how_long