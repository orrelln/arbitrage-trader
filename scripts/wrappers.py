from time import sleep, time


def timed_call(callback, sleep_length, calls, *args, **kw):
    """
    Calls a function a number of times with sleep inbetween each time
    """
    for _ in range(calls):
        sleep_time = time() + sleep_length
        callback(*args, **kw)
        if sleep_time > time():
            sleep(sleep_time - time())


def indef_call(callback, sleep_length, *args, **kw):
    """
    Calls a function indefinitely.
    """
    while True:
        sleep_time = time() + sleep_length
        callback(*args, **kw)
        if sleep_time > time():
            sleep(sleep_time - time())
