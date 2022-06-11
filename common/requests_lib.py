import os
import threading
import time
import requests
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:73.0) Gecko/20100101 Firefox/73.0',
    'X-Requested-With': 'XMLHttpRequest'
}


def self_poweroff(sleep):
    if sleep:
        time.sleep(sleep)
    os.system('Poweroff')


def poweroff(sleep):
    threading.Thread(target=self_poweroff, args=(sleep,)).start()


def get(url, headers=None):
    if headers is None:
        headers = HEADERS
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        if response.status_code == 403:
            poweroff(1)
        return {'status_code': response.status_code, 'text': response.text}
    return response.json()


def get_text(url, headers=None):
    if headers is None:
        headers = HEADERS
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        return f'error: {response.status_code}, {response.text}'
    return response.text


def post(url, data=None, json=None, headers=None):
    if headers is None:
        headers = HEADERS
    response = requests.post(url, data, json=json, headers=headers)
    if response.status_code != 200:
        return {'status_code': response.status_code, 'text': response.text}
    return response.json()
