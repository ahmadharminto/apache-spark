import configparser

from pyspark import SparkConf

def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)

    return spark_conf

def load_data_file(spark, datafile):
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(datafile)

    # format : csv, json, parquest, orc, jdbc, cassandra, mongodb, avro, xml, redshift, hbase
    # mode : PERMISSIVE, DROPMALFORMED, FAILFAST
    # schema : can be explicit, infer, implicit
    '''
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("path", datafile) \
        .option("mode", "FAILFAST")
        .load()
    '''

def count_by_country(df):
    filtered_df = df.where("Age < 40")
    selected_df = filtered_df.select("Age", "Gender", "Country", "state")
    grouped_df = selected_df.groupBy("Country")
    return grouped_df.count()