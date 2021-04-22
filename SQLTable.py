from pyspark.sql import *

from lib.logger import Log4J
from lib.utils import get_spark_app_config

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .appName("SQL Table") \
        .getOrCreate()

    logger = Log4J(spark)

    parquet_df = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")

    spark.sql("CREATE DATABASE IF NOT EXISTS MYDB")
    spark.catalog.setCurrentDatabase("MYDB")

    parquet_df.write \
        .format("csv") \
        .mode("overwrite") \
        .bucketBy(5, "OP_CARRIER", "ORIGIN") \
        .sortBy("OP_CARRIER", "ORIGIN") \
        .saveAsTable("my_table_output")

    logger.info(spark.catalog.listTables("MYDB"))