import os
import sys

sys.path.append(os.getcwd())
import findspark
from extract.extract import extract
from load.load import load
from transform.transform import transform

if __name__ == "__main__":
    # # load data from table article
    # dataframe = extract()
    # # transform data
    # transform(dataframe)
    # print("transform")
    # # save data to database
    # load(dataframe, "result")
    with open("../logs/domain.txt", "r", encoding="utf8") as file:
        data = [x.replace("\n", "") for x in file.readlines()]

    vietnamnet = extract(domain=data[1])
    dantri = extract(domain=data[2])
    tuoitre = extract(domain=data[3])

    transform(vietnamnet, data, tuoitre)
