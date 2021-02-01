from qrlogging import logger


def log_error(f):
    def wrapper(*args, **kwargs):
        try:
            s = f(*args, **kwargs)
            return s
        except Exception as e:
            logger.error(e)
    return wrapper


#@sql_request('error')
#def