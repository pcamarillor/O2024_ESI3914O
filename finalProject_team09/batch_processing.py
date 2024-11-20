# batch_processing.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, expr
from pyspark.sql.types import IntegerType, FloatType
import sys

def main():
    try:
        # Initialize SparkSession
        spark = SparkSession.builder \
            .appName("BatchProcessingPostgreSQL") \
            .config("spark.jars", "/Users/franciscofloresenriquez/O2024_ESI3914O/finalProject_team09/postgresql-42.7.4.jar") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("INFO")

        # Read parquet data
        input_path_clicks = "/tmp/parquet_data_clicks"
        input_path_views = "/tmp/parquet_data_views"

        clicks_df = spark.read.parquet(input_path_clicks)
        views_df = spark.read.parquet(input_path_views)

        # Show original schema
        print("Esquema original de clicks_df:")
        clicks_df.printSchema()

        # DataFrame trasnformations
        filtered_clicks = clicks_df.filter(col("event_type") == "click")
        filtered_views = views_df.filter(col("event_type") == "view")

        # Add 'click_id' column as UUID
        filtered_clicks = filtered_clicks.withColumn("click_id", expr("uuid()"))

        # add 'view_id' column as UUID
        filtered_views = filtered_views.withColumn("view_id", expr("uuid()"))

        # Convert 'timestamp' to TimestampType
        filtered_clicks = filtered_clicks.withColumn("timestamp", to_timestamp(col("timestamp")))
        filtered_views = filtered_views.withColumn("timestamp", to_timestamp(col("timestamp")))

        # Convert 'user_id' and 'click_count' to IntegerType
        filtered_clicks = filtered_clicks.withColumn("user_id", col("user_id").cast(IntegerType()))
        filtered_clicks = filtered_clicks.withColumn("click_count", col("click_count").cast(IntegerType()))

        # Convert 'user_id' and 'view_duration' to 'views' to correct types
        filtered_views = filtered_views.withColumn("user_id", col("user_id").cast(IntegerType()))
        filtered_views = filtered_views.withColumn("view_duration", col("view_duration").cast(FloatType()))

        # Show transformed schemas
        print("Schema transformed from filtered_clicks:")
        filtered_clicks.printSchema()

        print("Schema transformed from filtered_views:")
        filtered_views.printSchema()

        # Define JDBC connection properties
        jdbc_url = "jdbc:postgresql://localhost:5433/webActivity?stringtype=unspecified"
        jdbc_properties = {
            "user": "project",
            "password": "final",
            "driver": "org.postgresql.Driver"
        }

        # Write filtered dataframes into postgres
        filtered_clicks.write \
            .jdbc(url=jdbc_url, table="clicks", mode="append", properties=jdbc_properties)

        filtered_views.write \
            .jdbc(url=jdbc_url, table="views", mode="append", properties=jdbc_properties)

        print("Processed data written successfully to PostgreSQL")

        # Stop SparkSession
        spark.stop()

    except Exception as e:
        print("An error occurred during the batch processing:", file=sys.stderr)
        print(str(e), file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()