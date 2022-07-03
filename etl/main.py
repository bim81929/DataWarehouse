import os
import sys
import uuid

sys.path.append(os.getcwd())
import findspark
from extract.extract import extract
from load.load import load
from transform.transform import transform

if __name__ == "__main__":
    with open("../logs/domain.txt", "r", encoding="utf8") as file:
        data = [x.replace("\n", "") for x in file.readlines()]
        print(data)
    vietnamnet = extract(data[0])
    dantri = extract(data[1])
    tuoitre = extract(data[2])

    result = transform(vietnamnet, dantri, tuoitre)

    id = []
    for i in range(0, len(result["title"])):
        id.append(str(uuid.uuid4()))
    result.insert(1, "id", id, True)
    list_drop = ["level_0", "index", "url"]
    for _drop in list_drop:
        try:
            result.drop(_drop, axis=1, inplace=True)
        except:
            continue
    load(result, "result")
