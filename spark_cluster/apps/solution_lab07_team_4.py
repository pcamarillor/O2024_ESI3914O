from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import datetime
from pyspark.sql.functions import col, isnan, when, count, to_date

def clean_df(netflix_df) -> DataFrame:
    return netflix_df.dropna()

def write_df(netflix_df) -> None :
    return netflix_df.write.partitionBy('release_year', 'type').parquet('/opt/spark-data', mode='overwrite')

if __name__ == "__main__":
    # Initialize SparkSession
    spark = SparkSession \
                    .builder \
                    .appName("Lab 06 - RentalCarAnalysis") \
                    .config("spark.ui.port","4040") \
                    .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # Load Netflix DF
    netflix_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("/opt/spark-data/netflix1.csv")
    df_nulls = netflix_df \
                .select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in netflix_df.columns])
    
    df_nulls.show()

    netflix_df = clean_df(netflix_df)
    
    netflix_df.select([count(when(col(c).isNull(), c)).alias(c) for c in netflix_df.columns]).show()

    write_df(netflix_df) 