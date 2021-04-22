from pyspark.sql import *
from pyspark.sql.functions import *

from lib.utils import get_spark_app_config

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .appName("Process HTTP Log") \
        .getOrCreate()

    log_file = spark.read.text("data/apache_logs.txt")
    log_file.printSchema()
    log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'

    log_df = log_file.select(regexp_extract("value", log_reg, 1).alias("ip"),
                             regexp_extract("value", log_reg, 4).alias("date"),
                             regexp_extract("value", log_reg, 6).alias("request"),
                             regexp_extract("value", log_reg, 10).alias("referrer"))

    log_df.printSchema()

    log_df \
        .where("trim(referrer) != '-'") \
        .withColumn("referrer", substring_index("referrer", "/", 3)) \
        .groupBy("referrer") \
        .count() \
        .show(100, truncate=False)
