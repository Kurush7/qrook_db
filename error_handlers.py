from qrlogging import logger
import time

def log_error(f):
    def wrapper(*args, **kwargs):
        try:
            s = f(*args, **kwargs)
            return s
        except Exception as e:
            logger.exception(e)
    return wrapper


def log_error_default(default=None):
    def decorator(f):
        def wrapper(*args, **kwargs):
            try:
                s = f(*args, **kwargs)
                return s
            except Exception as e:
                logger.exception(e)
                return default
        return wrapper
    return decorator


def log_error_default_self(f):
    def wrapper(*args, **kwargs):
        try:
            s = f(*args, **kwargs)
            return s
        except Exception as e:
            logger.exception(e)
            return args[0]
    return wrapper


# todo param default from config
def retry_log_error(retry_delay=5):
    def decorator(f):
        def wrapper(*args, **kwargs):
            while 1:
                try:
                    s = f(*args, **kwargs)
                    return s
                except Exception as e:
                    logger.warning(str(e) + '; retrying in 5s...')
                    time.sleep(retry_delay)
        return wrapper
    return decorator


def log_class(decorator, exceptions=None):
    if exceptions is None:
        exceptions = []
    def logger(cls):
        for o in dir(cls):
            if o.startswith('__') or o in exceptions:
                continue
            a = getattr(cls, o)
            if hasattr(a, '__call__'):
                decorated_a = decorator(a)
                setattr(cls, o, decorated_a)
        return cls
    return logger
