import os
import sys

sys.path.append(os.getcwd())
import findspark

findspark.init()
from common.string_matching.string_matching import matching_data


def transform(dataframe_1, dataframe_2, dataframe_3):
    dataframe_1 = matching_data(dataframe_1, dataframe_2)
    dataframe_1 = matching_data(dataframe_1, dataframe_3)
    return dataframe_1
