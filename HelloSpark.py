import sys

from pyspark import SparkConf
from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_data_file, count_by_country

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
        #.appName("Hello Spark") \
        #.master("local[3]") \


    logger = Log4J(spark)
    # logger.info("Starting Hellospark")

    sample_df = load_data_file(spark=spark, datafile=sys.argv[1])

    logger.info("Using Spark Dataframe (like ORM)")
    partitioned_df = sample_df.repartition(2)
    counted_df = count_by_country(df=partitioned_df)
    # counted_df.show()
    logger.info(counted_df.collect())

    logger.info("Using Spark SQL")
    view_tbl = sample_df.createOrReplaceTempView("sample_tbl")
    counted_df = spark.sql("SELECT Country, COUNT(1) AS count FROM sample_tbl WHERE Age<40 GROUP BY Country")
    logger.info(counted_df.collect())

    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())

    input("Debugging @ localhost:4040")
    # spark.stop()
