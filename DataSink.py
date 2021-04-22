from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4J
from lib.utils import get_spark_app_config

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .appName("Data Sink") \
        .getOrCreate()

    logger = Log4J(spark)

    parquet_df = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")

    logger.info("Partitions before : " + str(parquet_df.rdd.getNumPartitions()))
    parquet_df.groupBy(spark_partition_id()) \
        .count() \
        .show()

    partitioned_df = parquet_df.repartition(5)
    logger.info("Partitions after : " + str(partitioned_df.rdd.getNumPartitions()))
    partitioned_df.groupBy(spark_partition_id()) \
        .count() \
        .show()

    partitioned_df.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path", "output/avro/") \
        .save()

    parquet_df.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "output/json/") \
        .option("maxRecordsPerFile", 10000) \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .save()

