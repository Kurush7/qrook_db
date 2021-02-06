from qrlogging import logger
import time

# todo add param - error string
def log_error(f):
    def wrapper(*args, **kwargs):
        try:
            s = f(*args, **kwargs)
            return s
        except Exception as e:
            logger.error(e)
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