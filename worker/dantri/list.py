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
DOMAIN = "dantri.com.vn"


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
    # https://dantri.com.vn/bat-dong-san/trang-2.htm
    url = f"https://dantri.com.vn/{category}/trang-{page}.htm"
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
    list_data = BeautifulSoup(raw_data, "html.parser").findAll("article", {"class": "article-item"})
    for data in list_data:
        # _category = data.find("div", {"class": "feature-box__content--brand"}).getText().strip()
        # _url_title = data.find("h3", {"class": "feature-box__content--title vnn-title"}).find("a")
        # _summary = data.find("div", {"class": "feature-box__content--desc"}).getText().strip()

        _url = data.find("a").get("href")
        _title = data.find("h3", {"class":"article-title"}).getText()
        _summary = data.find("div", {"class":"article-excerpt"}).find("a").getText()
        _category = data.find("h1", {"class":"title-page"}).find("a").getText()


        df = pd.DataFrame(
            {"id": [str(uuid.uuid4())], "domain": DOMAIN, "url": [f"https://{DOMAIN}{_url}"],
             "category": [str(_category)],
             "title": [_title],
             "summary": [_summary], "created_date": date})
        connect = sql.get_connect()
        sql.sql_insert(connect, "list", df)
        sql.sql_close(connect)

    return len(list_data)
