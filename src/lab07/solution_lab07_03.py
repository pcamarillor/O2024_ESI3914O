from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import datetime
from pyspark.sql.functions import col, isnan, when, count, to_date

def clean_df(netflix_df) -> DataFrame:

    netflix_df = netflix_df.fillna({
        'show_id': 'Unknown',
        'type': 'Unknown',
        'title': 'Unknown',
        'director': 'Unknown',
        'country': 'Unknown',
        'date_added': 'Unknown',
        'release_year': 0,
        'rating': 'Unknown',
        'duration': 'Unknown',
        'listed_in': 'Unknown'
    })

    return netflix_df


def write_df(netflix_df) -> None:
    netflix_df.write \
        .partitionBy("release_year", "type") \
        .parquet("/opt/spark-data/parquet_nuevo", mode="overwrite")
    return None