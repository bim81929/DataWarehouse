import os
import sys

sys.path.append(os.getcwd())
import findspark

findspark.init()
from common.string_matching.string_matching import matching_data
import uuid


def transform(dataframe_1, dataframe_2, dataframe_3):
    dataframe_1 = matching_data(dataframe_1.distinct().toPandas(), dataframe_2.distinct().toPandas())
    result = matching_data(dataframe_1, dataframe_3.distinct().toPandas())
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
    return result
