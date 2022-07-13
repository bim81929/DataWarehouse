
from bs4 import BeautifulSoup
import requests

def parse(url):
    req = requests.get(url)
    soup = BeautifulSoup(req.text, "html.parser")
    title = soup.find("h1").get_text()
    summary = soup.find("p", attrs={"class": "description"}).contents[-1]
    author = soup.find_all("strong")[-1].get_text()
    return title, summary, author
