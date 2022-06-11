import logging

from config import config

LOG_FORMAT = '%(asctime)s -- %(levelname)s -- %(filename)s -- %(message)s'


class Logging:
    LOGGING = {
        'CRITICAL': logging.CRITICAL,
        'ERROR': logging.ERROR,
        'WARNING': logging.WARNING,
        'INFO': logging.INFO,
        'DEBUG': logging.DEBUG,
        'NOTSET': logging.NOTSET,
    }

    def __init__(self):
        # Get log level from config, default is INFO
        logging_level = config.LOGGING_LEVEL or 'INFO'
        self.logging_level = self.LOGGING[logging_level]

        # Get path of log file to write
        log_file = config.LOGGING_FILE
        self.log_file = log_file
        self.log_format = config.LOG_FORMAT or LOG_FORMAT

    def get_logger(self):
        # Set format log to file
        if self.log_file:
            logging.basicConfig(filename=self.log_file,
                                filemode='a',
                                format=self.log_format,
                                datefmt='%Y-%m-%d %H:%M:%S',
                                level=self.logging_level)
        log = logging.getLogger("crawler")

        # Set format log to screen
        ch = logging.StreamHandler()
        ch.setFormatter(logging.Formatter(self.log_format))
        log.addHandler(ch)
        log.setLevel(self.logging_level)

        return log


logger = Logging().get_logger()
