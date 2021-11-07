import logging

_log_format = "%(asctime)s [%(levelname)s]: [%(app)s, %(name)s] - %(message)s"

def info(msg, *args):
    if logger:
        logger.info(msg, *args)

def warning(msg, *args):
    if logger:
        logger.warning(msg, *args)

def error(msg, *args):
    if logger:
        logger.error(msg, *args)

def exception(msg, *args):
    if logger:
        logger.exception(msg, *args)


class CustomFormatter(logging.Formatter):
    """Logging Formatter to add colors and count warning / errors"""
    grey = "\033[30m"
    yellow = "\033[33m"
    red = "\033[31m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    FORMATS = {
        logging.DEBUG: grey + _log_format + reset,
        logging.INFO: grey + _log_format + reset,
        logging.WARNING: yellow + _log_format + reset,
        logging.ERROR: red + _log_format + reset,
        logging.CRITICAL: bold_red + _log_format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def get_file_handler(filename, level):
    file_handler = logging.FileHandler(filename)
    file_handler.setLevel(level)
    file_handler.setFormatter(logging.Formatter(_log_format))
    return file_handler


def get_stream_handler(level):
    sh = logging.StreamHandler()
    sh.setLevel(level)
    sh.setFormatter(logging.Formatter(_log_format))
    sh.setFormatter(CustomFormatter())
    return sh


def create_logger(logger_name='default', app_name='app', level="INFO",
                  file: str = None, file_level="INFO"):
    logger = logging.getLogger(logger_name)
    [logger.removeHandler(h) for h in logger.handlers]
    logger.addHandler(get_stream_handler(level))
    if file:
        logger.addHandler(get_file_handler(file, file_level))

    logger = logging.LoggerAdapter(logger, {'app': app_name})
    logger.setLevel('INFO')
    return logger

logger = create_logger()