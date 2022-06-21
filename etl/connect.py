import os
import sys

sys.path.append(os.getcwd())
import findspark

findspark.init()
from pyspark.sql import SparkSession
from config import config

from common import spark_helper

appName = "Load data from ClickHouse: Learn Spark SQL"
spark = spark_helper.get_spark_session(appName, config.SPARK_MASTER_HOST, config.SPARK_MASTER_PORT, config.LIBRARY_JDBC)

df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://localhost:5432/{config.DB_NAME}") \
    .option("dbtable", "list") \
    .option("user", config.DB_USERNAME) \
    .option("password", config.DB_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.show()
