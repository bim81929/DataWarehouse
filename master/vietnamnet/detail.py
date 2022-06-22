import datetime
import os
import sys

sys.path.append(os.getcwd())

from worker.vietnamnet import detail
from master import master_runner
from config import config
from common.sql import sql

DOMAIN = "vietnamnet.vn"
if __name__ == "__main__":
    """
        - query ra các các bài báo ở bảng list theo domain mong muốn, có thời gian crawl trong hôm nay
        - detail.crawl gọi tới detail tương ứng
    """
    columns = ["id", "url"]
    # date = datetime.datetime.now() - datetime.timedelta(days=3)
    # Để dòng trên nếu có sẵn database crawl từ hôm trước
    date = datetime.datetime.now()
    condition = f"created_date='{date.strftime(config.DATE_TIME_FORMAT)}' and domain='{DOMAIN}'"
    connect = sql.get_connect()
    data = sql.sql_read_table(connect, "list", columns, condition)
    # print(data)
    master_runner.detail(data, detail.crawl)
    [('key1', 'value1'), ()]
