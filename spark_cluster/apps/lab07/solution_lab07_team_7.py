from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import datetime
from pyspark.sql.functions import col, isnan, when, count, to_date

def clean_df(netflix_df) -> DataFrame:
    return netflix_df.dropna()

def write_df(netflix_df) -> None:
    netflix_df.write.mode("overwrite").partitionBy("release_year", "type").parquet("/opt/spark-apps/lab07/output")
    return None

spark = SparkSession.builder \
    .appName("Netflix Data Cleaning") \
    .getOrCreate()

netflix_df = spark.read.csv("/opt/spark-data/netflix1.csv", header=True, inferSchema=True)

cleaned_df = clean_df(netflix_df)

write_df(cleaned_df)

output_df = spark.read.parquet("/opt/spark-apps/lab07/output")
output_df.show()