import os
import sys

sys.path.append(os.getcwd())
import findspark

findspark.init()
from config import config
from common import spark_helper


def extract():
    # create spark session
    appName = "Extract"
    spark_session = spark_helper.get_spark_session(appName, config.SPARK_MASTER_HOST, config.SPARK_MASTER_PORT,
                                                   config.LIBRARY_JDBC)
    # df = spark_helper.spark_read_table(spark_session, config.DB_HOST, config.DB_PORT,
    #                                    config.DB_NAME, "list")
    query = "select title, summary, created_date from list where category='Thời sự'"
    df = spark_helper.spark_read_query(spark_session, config.DB_HOST, config.DB_PORT,
                                       config.DB_NAME, query)
    df.show()
    # spark_helper.close_spark_session(spark_session)

    spark_session.stop()


if __name__ == "__main__":
    extract()
