import os
import sys

sys.path.append(os.getcwd())
import findspark

findspark.init()
from config import config
from common import spark_helper


def extract(use_table=True, table=None, query=None):
    # create spark session
    appName = "Extract"
    spark_session = spark_helper.get_spark_session(appName, config.SPARK_MASTER_HOST, config.SPARK_MASTER_PORT,
                                                   config.LIBRARY_JDBC)
    if use_table and table is not None:
        df = spark_helper.spark_read_table(spark_session, config.DB_HOST, config.DB_PORT,
                                           config.DB_NAME, table)
    elif not use_table and query is not None:
        df = spark_helper.spark_read_query(spark_session, config.DB_HOST, config.DB_PORT,
                                           config.DB_NAME, query)
    spark_helper.close_spark_session(spark_session)
    return df


if __name__ == "__main__":
    extract()
