import datetime
import os
import sys

sys.path.append(os.getcwd())

from worker.dantri import detail
from master import master_runner
from config import config
from common.sql import sql

DOMAIN = "dantri.com.vn"
if __name__ == "__main__":
    """
        - query ra các các bài báo ở bảng list theo domain mong muốn, có thời gian crawl trong hôm nay
        - detail.crawl gọi tới detail tương ứng
    """
    columns = ["id", "url"]
    date = datetime.datetime.now()
    condition = f"created_date='{date.strftime(config.DATE_TIME_FORMAT)}' and domain='{DOMAIN}'"
    connect = sql.get_connect()
    data = sql.sql_read_table(connect, "list", columns, condition)
    master_runner.detail(data, detail.crawl)
