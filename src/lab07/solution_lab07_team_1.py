from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import datetime
from pyspark.sql.functions import col, isnan, when, count, to_date


def clean_df(netflix_df) -> DataFrame:
    return netflix_df.dropna()


def write_df(netflix_df) -> None:
    netflix_df.write.partitionBy("release_year", "type").mode("overwrite").parquet(
        "./datasets/outputs/parquet_partitioned"
    )
    return None
