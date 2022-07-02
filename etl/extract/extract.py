import os
import sys

from common.spark_helper import spark_read_query

sys.path.append(os.getcwd())
import findspark

findspark.init()
from config import config
from common import spark_helper
from datetime import datetime


def extract(domain):
    # create spark session
    appName = f"Extract"
    spark_session = spark_helper.get_spark_session(appName, config.SPARK_MASTER_HOST, config.SPARK_MASTER_PORT,
                                                   config.LIBRARY_JDBC)
    query = f"select * from article where created_date<='{datetime.now().strftime(config.DATE_TIME_FORMAT)}'" \
            f"and domain='{domain}'"
    data = spark_read_query(spark_session, config.DB_HOST, config.DB_PORT, config.DB_NAME, query).distinct().toPandas()
    return data


if __name__ == "__main__":
    data = extract()
    data.show()
