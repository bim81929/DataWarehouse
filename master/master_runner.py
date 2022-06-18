from common import monitor_helper
from common.logging import logger
from config import config


def list(file_path, worker, fn_prepare=int):
    offset = int(config.WORKER_NUMS)
    with open(file_path) as file:
        for line in file:
            line = line.strip()
            if fn_prepare:
                line = fn_prepare(line)
            for page in range(1, offset + 1):
                print(f'crawl {file_path} -> {line} from {page}')
                worker.delay(line, page, offset)
    monitor_helper.wait_queue(check_count=1,
                              offset_time=2 * monitor_helper.SECONDS_IN_MINUTE,
                              fn_check_count=monitor_helper.check_stop_workers)


def detail(item_list, worker):
    count = 0
    wait_queue_count = 30 * int(config.WORKER_NUMS)
    for i, item in enumerate(item_list):
        worker.delay(item)
        count += 1
        if count >= wait_queue_count:
            logger.info(f'Wait at {i + 1} of {len(item_list)}')
            count = 0
            monitor_helper.wait_queue(check_count=1, count=wait_queue_count, time_sleep=10)
