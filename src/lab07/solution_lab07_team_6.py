from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import datetime
from pyspark.sql.functions import col, isnan, when, count, to_date
from pyspark.sql.types import IntegerType

def clean_df(netflix_df) -> DataFrame:
    netflix_df.printSchema()
    netflix_df = netflix_df.dropna()

    netflix_df.select([count(when(col(c).isNull(), c)).alias(c) for c in netflix_df.columns]).show()

    # Cast columns
    netflix_df = netflix_df.withColumn("date_added", to_date(netflix_df["date_added"], "yyyy-MM-dd"))
    netflix_df = netflix_df.withColumn("release_year", netflix_df["release_year"].cast(IntegerType()))

    # Drop nulls
    netflix_df = netflix_df.dropna()
    netflix_df.printSchema()

    return netflix_df

def write_df(netflix_df) -> None :
    df_parquet = spark.write \
    .partitionBy("release_year", "type") \
    .mode("overwrite") \
    .parquet("/opt/spark-data/output/netflix_parqued_partitioned")
    return df_parquet
