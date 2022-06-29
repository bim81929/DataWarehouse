import os
import sys

sys.path.append(os.getcwd())
import findspark

findspark.init()
from config import config
from common import spark_helper


def load(dataframe, table):
    appName = "Load"
    spark_session = spark_helper.get_spark_session(appName, config.SPARK_MASTER_HOST, config.SPARK_MASTER_PORT,
                                                   config.LIBRARY_JDBC)
    spark_helper.save_dataframe_to_postgreesql(dataframe, config.DB_HOST, config.DB_PORT, config.DB_NAME, table)
    spark_helper.close_spark_session(spark_session)
