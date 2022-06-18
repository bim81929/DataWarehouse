import datetime
import os
import sys

sys.path.append(os.getcwd())

from worker.vietnamnet import detail
from master import master_runner
from config import config
from common.sql import sql

if __name__ == "__main__":
    columns = ["id", "url"]
    # date = datetime.datetime.now() - datetime.timedelta(days=1)
    # Để dòng trên nếu có sẵn database crawl từ hôm trước
    date = datetime.datetime.now()
    condition = f"created_date='{date.strftime(config.DATE_TIME_FORMAT)}'"
    connect = sql.get_connect()
    data = sql.sql_read_table(connect, "list", columns, condition)
    # print(data)
    master_runner.detail(data, detail.crawl)
