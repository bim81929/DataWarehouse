import os
import sys

sys.path.append(os.getcwd())
import findspark
from extract.extract import extract
from load.load import load
from transform.transform import transform

if __name__ == "__main__":
    with open("../logs/result_category.txt", "r", encoding="utf8") as f:
        data = [x.replace("\n", "") for x in f.readlines()]

    for category in data:
        df = extract(category)
        df = transform(df)
        load(df, "result")
