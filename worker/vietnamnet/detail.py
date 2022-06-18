import uuid
from datetime import datetime

from common import requests_lib
from common.celery.app import app
from common.sql import sql
from bs4 import BeautifulSoup
from config import config
import pandas as pd

DOMAIN = "vietnamnet.vn"


@app.task(rate_limit='60/m',
          autoretry_for=(Exception,),
          retry_kwargs={'max_retries': 3},
          retry_backoff=True,
          retry_jitter=250)
def crawl(product_raw):
    raw_data = requests_lib.get_text(product_raw[1])
    data = BeautifulSoup(raw_data, "html.parser")
    if 'KHÔNG TÌM THẤY ĐƯỜNG DẪN Này' not in data:
        parser(product_raw[0], data)
    return len(raw_data)


@app.task(autoretry_for=(Exception,),
          retry_kwargs={'max_retries': 3},
          retry_backoff=True,
          retry_jitter=250)
def parser(raw_id, data):
    category_and_date = data.find("div", {"class": "breadcrumb-box flex-wrap"})
    url = data.find("link", {"rel": "alternate"}).get("href")
    category = category_and_date.find("a").getText()
    date_submitted = category_and_date.find("span").getText().strip().split(" ")[0].replace("/", "-")
    article = data.find("div", {"class": "newsFeatureBox"})
    title = article.find("div", {"class": "newsFeature__header"}).find("h1").getText()
    summary = article.find("div", {"class": "newFeature__main-textBold"}).getText()
    text = [t.getText() for t in article.findAll("p")]
    author = text[-1]
    description = "\n".join(text)
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
    sql.sql_close(connect)
    return len(data)
