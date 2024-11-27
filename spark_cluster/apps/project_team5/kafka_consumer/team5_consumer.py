import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, min, max
from pyspark.sql.types import StructField, StructType, StringType, TimestampType, DoubleType

def consume_kafka_events(kafka_server, topics):
    """
    Function to consume Kafka events from specified topics using PySpark Structured Streaming
    and store the processed data into a data lake with console logging.

    Args:
        kafka_server (str): Kafka bootstrap server (e.g., ad9bbd76f991:9093).
        topics (str): Comma-separated list of Kafka topics to subscribe to.
        data_lake_path (str): Path to store the processed data in the data lake.
        checkpoint_path (str): Path for storing the checkpoint data.
    """
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("KafkaExchangeRateConsumer") \
        .config("spark.ui.port", "4040") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print(f"Starting Kafka Consumer for topics: {topics}")

    # Schema for the exchange rate data
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("currency_pair", StringType(), True),
        StructField("rate", DoubleType(), True)
    ])

    # Read Kafka stream
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", topics) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    print("Kafka Stream Schema:")
    kafka_df.printSchema()

    # Parse Kafka message value as JSON
    parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data"))
    exchange_rates = parsed_df.select("data.*")

    # Add timestamp column as Spark TimestampType
    exchange_rates = exchange_rates.withColumn("timestamp", col("timestamp").cast(TimestampType()))

    # Add watermark to handle late data
    exchange_rates_with_watermark = exchange_rates.withWatermark("timestamp", "5 minutes")

    # Perform windowed aggregations with watermark
    windowed_aggregates = exchange_rates_with_watermark.groupBy(
        window(col("timestamp"), "1 minute"),
        col("currency_pair")
    ).agg(
        avg("rate").alias("avg_rate"),
        min("rate").alias("min_rate"),
        max("rate").alias("max_rate")
    )

    print("Windowed Aggregates Schema:")
    windowed_aggregates.printSchema()

    # Write the processed data to the data lake
    query = windowed_aggregates.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "/otp/spark-data/parquet/exchange_rates") \
        .option("checkpointLocation", "/otp/spark-data/parquet/checkpoint") \
        .trigger(processingTime="1 minute") \
        .start()

    print("Saving data to Data Lake at /otp/spark-data/parquet/exchange_rates")
    print("Checkpointing at /otp/spark-data/parquet/checkpoint")

    # Await termination of the streaming query
    query.awaitTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Kafka Consumer")
    parser.add_argument('--kafka-bootstrap', required=True, help="Kafka bootstrap server")
    parser.add_argument('--kafka-topics', required=True, help="Comma-separated Kafka topics to subscribe to")
    args = parser.parse_args()

    consume_kafka_events(args.kafka_bootstrap, args.kafka_topics)
