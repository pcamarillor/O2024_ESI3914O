from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, isnan, when, count, to_date

def clean_df(netflix_df) -> DataFrame:
    netflix_df.printSchema()

    netflix_df = netflix_df.\
            withColumn("date_added", to_date(netflix_df["date_added"], "yyyy-MM-dd")) \
            .withColumn("release_year", netflix_df["release_year"].cast(IntegerType()))

    netflix_df.printSchema()

    return netflix_df.dropna()

def write_df(netflix_df) -> None :
    netflix_df.write \
        .mode("overwrite") \
        .partitionBy("release_year", "type") \
        .parquet("/opt/spark-data/output/netflix_data")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Lab 07 - Persisting Netflix data") \
        .config("spark.driver.bindAddress", "localhost") \
        .getOrCreate()

    netflix_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("/opt/spark-data/netflix1.csv")

    netflix_df = clean_df(netflix_df)
    write_df(netflix_df)
    netflix_df.show()
