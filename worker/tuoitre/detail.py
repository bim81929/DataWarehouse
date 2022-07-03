import uuid
from datetime import datetime

from common import requests_lib
from common.celery.app import app
from common.sql import sql
from bs4 import BeautifulSoup
from config import config
import pandas as pd

DOMAIN = "tuoitre.vn"


@app.task(rate_limit='60/m',
          autoretry_for=(Exception,),
          retry_kwargs={'max_retries': 3},
          retry_backoff=True,
          retry_jitter=250)
def crawl(product_raw):
    """
        - requests tới url được truyền vào, nếu url không lỗi, gọi hàm parser để xử lý và lưu vào database
    :param product_raw:
    :return:
    """
    raw_data = requests_lib.get_content(product_raw[1])
    data = BeautifulSoup(raw_data, "html.parser")
    if 'KHÔNG TÌM THẤY ĐƯỜNG DẪN Này' not in data:
        parser(product_raw[0], data, product_raw[1])
    return len(raw_data)


@app.task(autoretry_for=(Exception,),
          retry_kwargs={'max_retries': 3},
          retry_backoff=True,
          retry_jitter=250)
def parser(raw_id, data, url):
    """
        - bóc tách dữ liệu bằng BeautifulSoup
        - lưu vào database
    :param raw_id:
    :param data:
    :return:
    """
    category = data.find("div", {"class": "bread-crumbs fl"}).findAll("li")[0].findNext("a").getText()
    date_submitted = _convert_date(data.find("div", {"class": "date-time"}).getText().strip().split(" ")[0])

    title = data.find("h1", {"class": "article-title"}).getText()
    summary = data.find("h2", {"class": "sapo"}).getText().replace("\n", "").strip()
    text = [t.getText().replace("\n", "").strip() for t in data.find("div", {"id": "main-detail-body"})]
    description = " ".join(text)
    author = data.find("div", {"class": "author"}).getText().replace("\r", "\n").replace("\n", "")
    created_date = datetime.now().strftime(config.DATE_TIME_FORMAT)

    df = pd.DataFrame(
        {"id": [str(uuid.uuid4())], "raw_id": [raw_id], "domain": DOMAIN,
         "url": [url],
         "category": [category],
         "title": [title], "author": [author],
         "summary": [summary], "description": [description], "date_submitted": [date_submitted],
         "created_date": created_date})
    connect = sql.get_connect()
    sql.sql_insert(connect, "article", df)
    return len(data)


def _convert_date(date):
    list_date = list(date.split("/"))
    list_date.reverse()
    return "-".join(list_date)
