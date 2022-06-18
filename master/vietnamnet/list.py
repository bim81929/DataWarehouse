import os
import sys

sys.path.append(os.getcwd())
from worker.vietnamnet import list
from master import master_runner

if __name__ == "__main__":
    master_runner.list(file_path='D:\\DataWarehouse\\logs\\vietnamnet.txt', worker=list.crawl, fn_prepare=None)
