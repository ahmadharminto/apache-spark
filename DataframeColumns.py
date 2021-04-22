from pyparsing import col
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from lib.logger import Log4J
from lib.utils import get_spark_app_config

def to_date_df(df, format, field):
    return df.withColumn(field, to_date(col(field), format))

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .appName("DF Columns") \
        .getOrCreate()

    logger = Log4J(spark)

    sample_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("samplingRatio", "0.0001") \
        .load("data/flight*.csv")

    # we can access columns in 2 ways :
    # 1. column string
    # 2. column object

    sample_df.select("OP_CARRIER_FL_NUM", column("ORIGIN"), sample_df.ORIGIN_CITY_NAME, col("DEST_CITY_NAME")).show(10)

    # we can create column expression in 2 ways :
    # 1. String exp / SQL exp
    sample_df.select("OP_CARRIER_FL_NUM", expr("to_date(FL_DATE, 'm/d/yyyy') as FL_DATE")).show(10)
    # 2. Column object exp
    sample_df.select("OP_CARRIER_FL_NUM", to_date("FL_DATE", 'm/d/yyyy').alias("FL_DATE")).show(10)