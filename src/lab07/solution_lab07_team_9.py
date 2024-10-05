from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date


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
