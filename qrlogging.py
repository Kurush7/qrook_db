import logging
# todo use configs for filename and logger name


_log_format = "%(asctime)s [%(levelname)s]: [%(app)s, %(name)s] - " \
         "(%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"


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


def get_file_handler():
    file_handler = logging.FileHandler("qrook_app.log")
    file_handler.setLevel(logging.WARNING)
    file_handler.setFormatter(logging.Formatter(_log_format))
    return file_handler


def get_stream_handler():
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(logging.Formatter(_log_format))
    sh.setFormatter(CustomFormatter())
    return sh


def default_logger():
    '''fast setup
    logging.basicConfig(filename="qrook_app.log", level=logging.INFO,
                        format = "%(asctime)s - [%(levelname)s] - %(app)s - %(name)s -"\
                         "(%(filename)s).%(funcName)s(%(lineno)d) - %(message)s")'''

    logger = logging.getLogger("default")
    logger.addHandler(get_file_handler())
    logger.addHandler(get_stream_handler())
    logger = logging.LoggerAdapter(logger, {"app": "qrook"})
    return logger


logger = default_logger()
