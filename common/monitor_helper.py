import base64
import os
import time
from datetime import datetime

from common import requests_lib
from common.logging import logger
from config import config

SECONDS_IN_MINUTE = 60


def get_message_count():
    token = base64.b64encode(f'admin:{config.MASTER_PASSWORD}'.encode('utf8')).decode("ascii")
    headers = {'Authorization': f'Basic {token}'}
    data = requests_lib.get(f'http://{config.MASTER_HOST}:5555/api/queues/length', headers)
    return int(data['active_queues'][0]['messages'])


def wait_queue(check_count=0, offset_time=0, count=0, time_sleep=SECONDS_IN_MINUTE, fn_check_count=None):
    my_offset_time = 0
    if offset_time:
        my_offset_time = datetime.now().timestamp()
    my_check_count = 0
    last_count = 0
    while True:
        try:
            counter = get_message_count()
            if (count > 0 and counter <= count) \
                    or counter == last_count \
                    or (my_offset_time > 0 and (datetime.now().timestamp() - my_offset_time) > offset_time):
                logger.info(f'Ok {check_count} Remaining {counter} requests count {my_check_count}')
                my_check_count += 1
                if my_check_count > check_count:
                    if fn_check_count is not None:
                        fn_check_count(counter)
                    return counter
            else:
                logger.info(f'Wait {check_count} Remaining {counter} requests count {my_check_count}')
                my_check_count = 0
                last_count = counter
        except Exception as e:
            logger.warn(e)
        time.sleep(time_sleep)


def start_workers(num_worker=None):
    my_num_worker = num_worker
    if my_num_worker is None:
        my_num_worker = config.WORKER_NUMS
    if my_num_worker is not None:
        token = base64.b64encode(config.MASTER_PASSWORD.encode('utf8')).decode('utf8')
        headers = {"Authorization": token}
        url = f'http://{config.MASTER_HOST}:8889/api/scaling'
        scaling = requests_lib.get(url, headers=headers)
        logger.info(scaling)
        if scaling['required'] < my_num_worker:
            logger.info(f'Start {my_num_worker} worker(s)')
            request_body = {
                "min": "0",
                "required": "{}".format(my_num_worker),
                "max": "{}".format(max(my_num_worker, scaling['max'])),
            }
            try:
                requests_lib.patch(url, json=request_body, headers=headers)
            except Exception as e:
                logger.warn(e)


def stop_workers():
    logger.info('Preparing end queue')
    logger.info('Stop all workers')
    token = base64.b64encode(config.MASTER_PASSWORD.encode('utf8')).decode('utf8')
    headers = {"Authorization": token}
    url = f'http://{config.MASTER_HOST}:8889/api/scaling'
    scaling = requests_lib.get(url, headers=headers)
    logger.info(scaling)
    request_body = {
        "min": "0",
        "required": "0",
        "max": "{}".format(scaling['max']),
    }
    try:
        requests_lib.patch(url, json=request_body, headers=headers)
    except Exception as e:
        logger.warn(e)
    request_body = {
        'draw': 1,
        'order[0][column]': 7,
        'start': 0,
        'length': 10,
        'search[value]': 'state:FAILURE'
    }
    url = f'http://{config.MASTER_HOST}:5555/tasks/datatable'
    token = base64.b64encode(f'admin:{config.MASTER_PASSWORD}'.encode('utf8')).decode("ascii")
    headers = {'Authorization': f'Basic {token}'}
    data = requests_lib.post(url, data=request_body, headers=headers)
    if data is not None and not data['recordsTotal']:
        time.sleep(10)
        os.system('sudo systemctl restart flower')
    if config.MASTER_POWEROFF:
        time.sleep(60)
        logger.info('All done!')
        logger.info('Preparing power off')
        os.system('sudo poweroff')


def check_start_workers(counter):
    if counter > 0:
        start_workers()


def check_stop_workers(counter):
    if counter == 0:
        stop_workers()
