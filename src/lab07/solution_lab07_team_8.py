from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import datetime
from pyspark.sql.functions import col, isnan, when, count, to_date

def clean_df(netflix_df) -> DataFrame:
    cleaned_df = netflix_df.dropna()

    return cleaned_df

def write_df(netflix_df) -> None :
    netflix_df.write.mode('overwrite').csv('output/netflix_cleaned.csv', header=True)
