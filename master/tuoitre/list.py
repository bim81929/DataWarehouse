import os
import sys

sys.path.append(os.getcwd())
from worker.tuoitre import list
from master import master_runner

if __name__ == "__main__":
    """
        - file_path: file chứa các category(các mục báo), cấu trúc <domain>.txt ở logs
        - worker: gọi tới list.crawl tương ứng
    """
    master_runner.list(file_path='logs/tuoitre.txt', worker=list.crawl, fn_prepare=None)
