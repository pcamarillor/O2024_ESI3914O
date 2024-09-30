from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import datetime
from pyspark.sql.functions import col, isnan, when, count, to_date

def clean_df(netflix_df) -> DataFrame:
    netflix_df = netflix_df.fillna({
        'rating': 'Not Rated',      
        'duration': 'Unknown',      
        'country': 'Unknown',       
        'director': 'Unknown',      
        'date_added': 'Unknown',    
        'listed_in': 'Unknown'      
    })

    # Poner el año de lanzamiento en 1900 si no es un número
    netflix_df = netflix_df.withColumn('release_year',when(col('release_year').cast('int').isNull(), 1900).otherwise(col('release_year').cast('int'))
    )


    netflix_df = netflix_df.dropna(subset=['show_id', 'type', 'title'])
    return netflix_df



def write_df(netflix_df) -> None :
    netflix_df.write \
        .partitionBy("release_year", "type") \
        .mode("overwrite") \
        .parquet("/opt/spark-data/output/netflix_partitioned")