import functools
import datetime


def exception_catch(log_path):
    def deco(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                with open(log_path, 'a') as log:
                    log.write('{} {} {} {} {}\n'.format(datetime.datetime.now(),
                                                        type(e), e, args, kwargs))
                # This will return None on error, of course
        return wrapper
    return deco
