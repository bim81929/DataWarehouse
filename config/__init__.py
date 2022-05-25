import os


class Config:
    CELERY_APP = 'common.celery.app'
    # CELERY_TASKS = 'worker.sample.hello'
    LOGGING_LEVEL = 'INFO'
    LOGGING_FILE = 'crawler.log'
    LOG_FORMAT = '%(asctime)s -- %(levelname)s -- %(filename)s -- %(message)s'
    # MASTER_RUNNERS = 'master/sample/hello.py'
    MASTER_HOST = 'localhost'
    MASTER_PASSWORD = ''
    WORKER_NUMS = 1
    MASTER_POWEROFF = False
    CONFIG_FOLDER = 'logs'
    HDFS_HOST = "127.0.0.1"
    HDFS_PORT = "50070"

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
