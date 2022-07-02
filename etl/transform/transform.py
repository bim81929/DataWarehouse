import os
import sys

sys.path.append(os.getcwd())
import findspark

findspark.init()
from config import config
from common import spark_helper
from datetime import datetime


def transform(dataframe):


    return dataframe
