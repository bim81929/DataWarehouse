from celery import Celery

from common.logging import logger
from config import config


class App:
    def __init__(self, broker=None, tasks=None, backend=None):
        self.broker = broker
        self.tasks = tasks
        self.backend = backend

    def create_app(self, app_name):
        logger.info(f'{app_name}: broker={self.broker}, tasks={self.tasks}, backend={self.backend}')
        celery_app = Celery(app_name, broker=self.broker, include=self.tasks, backend=self.backend)
        return celery_app


def init_app(app_name):
    broker = f'redis://{config.MASTER_HOST}:6379/0'
    tasks = config.CELERY_TASKS
    if not broker or not tasks:
        return App()
    tasks = tasks.split(',')
    celery = App(broker=broker, tasks=tasks, backend=f'redis://{config.MASTER_HOST}:6379/1')
    celery_app = celery.create_app(app_name=app_name)
    return celery_app


app = init_app(app_name='tasks')
