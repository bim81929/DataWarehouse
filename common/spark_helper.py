import findspark

findspark.init()

from pyspark.sql import SparkSession
from config import config


def get_spark_session(name, host, port, lib):
    spark = SparkSession.builder \
        .master(f'spark://{host}:{port}').appName(name) \
        .config("spark.jars", lib)
    return spark.getOrCreate()


def close_spark_session(spark_session):
    spark_session.stop()


def spark_read_query(spark_session, host, port, database, query, temporary_table=False, name_of_temporary_table=None):
    df = spark_session.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{host}:{port}/{database}") \
        .option("query", query) \
        .option("user", config.DB_USERNAME) \
        .option("password", config.DB_PASSWORD) \
        .option("driver", config.JDBC_DRIVER) \
        .load()
    if temporary_table and name_of_temporary_table is not None:
        return df.createTempView(name_of_temporary_table)
    return df


def spark_read_table(spark_session, host, port, database, table, temporary_table=False, name_of_temporary_table=None):
    df = spark_session.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{host}:{port}/{database}") \
        .option("dbtable", table) \
        .option("user", config.DB_USERNAME) \
        .option("password", config.DB_PASSWORD) \
        .option("driver", config.JDBC_DRIVER) \
        .load()
    if temporary_table and name_of_temporary_table is not None:
        return df.createTempView(name_of_temporary_table)
    return df


def save_dataframe_to_postgreesql(dataframe, table):
    dataframe.write.format("jdbc") \
        .option("url", f"jdbc:postgresql://{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}") \
        .option("driver", config.JDBC_DRIVER).option("dbtable", table) \
        .option("user", config.DB_USERNAME).option("password", config.DB_PASSWORD).mode('append').save()
