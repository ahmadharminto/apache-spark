from datetime import date
from unittest import TestCase

from pyspark import Row
from pyspark.sql import SparkSession

from DataframeRows import to_date_df
from lib.utils import load_data_file, count_by_country
from pyspark.sql.functions import *
from pyspark.sql.types import *

class UtilsTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .master("local[3]") \
            .appName("HelloSparkTest") \
            .getOrCreate()

    def test_datafile_loading(self):
        sample_df = load_data_file(self.spark, "data/sample.csv")
        result_count = sample_df.count()
        self.assertEqual(result_count, 9, "Total record should be 9")

    def test_country_count(self):
        sample_df = load_data_file(self.spark, "data/sample.csv")
        count_list = count_by_country(sample_df).collect()
        count_dict = dict()
        for row in count_list:
            count_dict[row["Country"]] = row["count"]
        self.assertEqual(count_dict["United States"], 4, "US should be 4")
        self.assertEqual(count_dict["United Kingdom"], 1, "UK should be 1")
        self.assertEqual(count_dict["Canada"], 2, "CA should be 2")

class DFRowsTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .master("local[3]") \
            .appName("DFRowsTest") \
            .getOrCreate()

        my_schema = StructType([
            StructField("ID", StringType()),
            StructField("EventDate", StringType())
        ])

        my_rows = [Row("123", "04/05/2020"), Row("124", "04/5/2020"), Row("125", "04/05/2020"),
                   Row("126", "04/05/2020")]
        my_rdd = cls.spark.sparkContext.parallelize(my_rows, 2)
        cls.my_df = cls.spark.createDataFrame(my_rdd, my_schema)

    def test_data_type(self):
        rows = to_date_df(self.my_df, "M/d/y", "EventDate").collect()
        for row in rows:
            self.assertIsInstance(row["EventDate"], date)