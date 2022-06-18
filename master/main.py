import os
import sys

sys.path.append(os.getcwd())
from worker.vietnamnet import list


def run():
    category = ["thoi-su"]
    list.crawl.delay(category[0])


if __name__ == "__main__":
    run()
