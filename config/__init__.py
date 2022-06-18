import os


class Config:
    CELERY_APP = 'common.celery.app'
    CELERY_TASKS = 'worker.vietnamnet.list'
    LOGGING_LEVEL = 'INFO'
    LOGGING_FILE = 'crawler.log'
    LOG_FORMAT = '%(asctime)s -- %(levelname)s -- %(filename)s -- %(message)s'
    MASTER_RUNNERS = 'master/main.py'
    MASTER_HOST = 'localhost'
    MASTER_PASSWORD = ""
    WORKER_NUMS = 1
    CONFIG_FOLDER = 'logs'

    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_USERNAME = "dev"
    DB_PASSWORD = "2205"
    DB_NAME = "thdl"
    DATE_TIME_FORMAT = "%Y-%m-%d"

    def __init__(self):
        # Set all values to config
        for k in os.environ:
            value = None
            try:
                value = getattr(self, k)
            except:
                pass
            setattr(self, k, os.environ[k])

    def get_value(self, value):
        try:
            return getattr(self, value)
        except:
            return None


config = Config()
