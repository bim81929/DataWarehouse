import uuid
from datetime import datetime

from common import sql
from common.celery.app import app
from common import requests_lib
from bs4 import BeautifulSoup

@app.task(rate_limit='60/m',
          autoretry_for=(Exception,),
          retry_kwargs={'max_retries': 3},
          retry_backoff=True,
          retry_jitter=250)
def crawl():
    page = 0
    while True:
        url = f"https://www.thegioididong.com/dtdd#c=42&o=9&pi={page}"
        data = requests_lib.get_text(url)
        page += 1