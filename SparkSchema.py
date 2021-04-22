from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType

from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_data_file, count_by_country

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .appName("Spark Schema") \
        .getOrCreate()

    logger = Log4J(spark)

    data_schema = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    ddl_schema = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
          ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
          WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""

    csv_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("mode", "FAILFAST") \
        .option("dateFormat", "M/d/y") \
        .schema(data_schema) \
        .load("data/flight*.csv")

    csv_df.show(5)
    logger.info("CSV schema : ")
    logger.info(csv_df.schema.simpleString())

    # -----------------------------

    json_df = spark.read \
        .format("json") \
        .option("dateFormat", "M/d/y") \
        .schema(ddl_schema) \
        .load("data/flight*.json")

    json_df.show(5)
    logger.info("JSON schema : ")
    logger.info(json_df.schema.simpleString())

    # -----------------------------

    parquet_df = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")

    parquet_df.show(5)
    logger.info("Parquet schema : ")
    logger.info(parquet_df.schema.simpleString())

    input("Debugging @ localhost:4040")
