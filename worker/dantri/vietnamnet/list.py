import json
import uuid
from datetime import datetime
import sys
import pandas as pd

from common.sql import sql
from common.celery.app import app
from common import requests_lib
from bs4 import BeautifulSoup
from config import config

LIMIT_CRAWL = 100
DOMAIN = "vietnamnet.vn"


@app.task(rate_limit='60/m',
          autoretry_for=(Exception,),
          retry_kwargs={'max_retries': 3},
          retry_backoff=True,
          retry_jitter=250)
def crawl(category, page, offset):
    """
        - hàm crawl: requets, trả về response, gọi đệ quy crawl và gọi hàm xử lý
        - tiến hành requests để lấy danh sách các bài báo theo chủ đề (chỉ crawl 100 trang đầu)
        - crawl.delay: gọi đệ quy
        _ parse: tiến hành xử lý response, lưu vào database
    :param category:
    :param page:
    :param offset:
    :return:
    """
    if page == 100:
        return None
    url = f"https://vietnamnet.vn/{category}-page{page}"
    response = requests_lib.get_text(url)
    _date = datetime.now().strftime(config.DATE_TIME_FORMAT)
    parse(response, _date)
    crawl.delay(category, page + offset, offset)
    return url


@app.task(autoretry_for=(Exception,),
          retry_kwargs={'max_retries': 3},
          retry_backoff=True,
          retry_jitter=250)
def parse(raw_data, date):
    """
    - tiến hành duyệt từng bài báo con trong requests trả về
    - lấy các trường và lưu vào dataframe
    :param raw_data:
    :param date:
    :return:
    """
    list_data = BeautifulSoup(raw_data, "html.parser").findAll("div", {"class": "feature-box"})
    for data in list_data:
        _category = data.find("div", {"class": "feature-box__content--brand"}).getText().strip()
        _url_title = data.find("h3", {"class": "feature-box__content--title vnn-title"}).find("a")
        _summary = data.find("div", {"class": "feature-box__content--desc"}).getText().strip()

        df = pd.DataFrame(
            {"id": [str(uuid.uuid4())], "domain": DOMAIN, "url": [f"https://{DOMAIN}{_url_title.get('href')}"],
             "category": [str(_category)],
             "title": [_url_title.getText().strip().replace("'", '"')],
             "summary": [str(_summary).replace("'", '"')], "created_date": date})
        connect = sql.get_connect()
        sql.sql_insert(connect, "list", df)
        sql.sql_close(connect)

    return len(list_data)
