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
def crawl(category):
    for page in range(0, LIMIT_CRAWL):
        url = f"https://vietnamnet.vn/{category}-page{page}"
        response = requests_lib.get_text(url)
        _date = datetime.now().strftime(config.DATE_TIME_FORMAT)
        parse(response, _date)
    return len(id)


@app.task(autoretry_for=(Exception,),
          retry_kwargs={'max_retries': 3},
          retry_backoff=True,
          retry_jitter=250)
def parse(raw_data, date):
    list_data = BeautifulSoup(raw_data, "html.parser").findAll("div", {"class": "feature-box"})
    for data in list_data:
        _category = data.find("div", {"class": "feature-box__content--brand"}).getText().strip()
        _url_title = data.find("h3", {"class": "feature-box__content--title vnn-title"}).find("a")
        _summary = data.find("div", {"class": "feature-box__content--desc"}).getText().strip()

        df = pd.DataFrame(
            {"id": [str(uuid.uuid4())], "domain": DOMAIN, "url": [_url_title.get("href")], "category": [str(_category)],
             "title": [_url_title.getText().strip().replace("'", '"')],
             "summary": [str(_summary).replace("'", '"')], "created_date": date})
        connect = sql.get_connect()
        sql.sql_insert(connect, "list", df)
        sql.sql_close(connect)

    return len(list_data)
