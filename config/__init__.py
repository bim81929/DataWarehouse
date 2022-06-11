import os


class Config:
    CELERY_APP = 'common.celery.app'
    CELERY_TASKS = 'worker.sample.hello'
    LOGGING_LEVEL = 'INFO'
    LOGGING_FILE = 'crawler.log'
    LOG_FORMAT = '%(asctime)s -- %(levelname)s -- %(filename)s -- %(message)s'
    MASTER_RUNNERS = 'master/sample/hello.py'
    MASTER_HOST = 'localhost'

    CONFIG_FOLDER = 'logs'

    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_USERNAME = "dev"
    DB_PASSWORD = "2205"
    DB_NAME = "thdl"

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
