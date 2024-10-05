from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import datetime
from pyspark.sql.functions import col, isnan, when, count, to_date


def clean_df(netflix_df) -> DataFrame:
    # We parse the date_added column to Date
    netflix_df_clean = netflix_df.withColumn("date_added", to_date(col("date_added"), "MMMM d, yyyy"))

    # We eliminate the rows with null values in any row
    netflix_df_clean = netflix_df_clean.dropna(how='any')

    return netflix_df_clean

def write_df(netflix_df) -> None:
    # Write the DF in Parquet format, partitioned by release_year and type
    netflix_df.write \
        .mode("overwrite") \
        .partitionBy("release_year", "type") \
        .parquet("output/netflix_data")
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