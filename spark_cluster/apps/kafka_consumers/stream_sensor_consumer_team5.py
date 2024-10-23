import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, explode, split, from_json, col, avg, max
from pyspark.sql.types import StructType, StringType, TimestampType, DoubleType, StructField

def consume_kafka_events(kafka_server):
    # Initialize SparkSession
    spark = SparkSession.builder \
                .appName("Structured-Streaming-Sensor-Example") \
                .config("spark.ui.port","4040") \
                .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    # Define the schema of the incoming JSON data
    sensor_schema = StructType([StructField("sensor_id", StringType(), True),
                                StructField("event_time", StringType(), True),
                                StructField("temperature", DoubleType(), True)
    ])

    # Create DataFrame representing the stream of input studens from file
    kafka_bootstrap_server = "{0}:9093".format(kafka_server)
    print("Establishing connection with {0}".format(kafka_bootstrap_server))
    kafka_df = spark \
        .readStream\
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
        .option("subscribe", "pyspark-example") \
        .option("startingOffsets", 
                # latest, earliest, default: latest
                "latest") \
        .load()

    kafka_df.printSchema()

    # Transform binary data to string
    sensor_data_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")

    # Parse the JSON string into separate columns based on the schema
    sensor_data_df = sensor_data_df.withColumn("sensor_data", from_json(sensor_data_df.json_value, sensor_schema))

    # Select the parsed fields (sensor_id, event_time, temperature)
    sensor_data_df = sensor_data_df.select("sensor_data.*")

    # Convert the event_time to a TimestampType (for time-based operations)
    sensor_data_df = sensor_data_df.withColumn("event_time", sensor_data_df.event_time.cast(TimestampType()))

    # Add a watermark of 5 minutes to allow late data
    sensor_data_df = sensor_data_df.withWatermark("event_time", "5 minutes")

    windowed_aggregates_df = sensor_data_df.groupBy(
        window(col("event_time"), "30 seconds"), 
        col("sensor_id")
    ).agg(
        avg("temperature").alias("avg_temperature"),
        max("temperature").alias("max_temperature")
    )

    # Output the results to the console
    query = windowed_aggregates_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("sensor_id"),
        col("avg_temperature"),
        col("max_temperature")
    ).writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Await termination to keep the streaming query running
    query.awaitTermination(20)

    print("stream closed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Kafka arguments")
    parser.add_argument('--kafka-bootstrap', required=True, help="Kafka bootstrap server")
    
    args = parser.parse_args()

    consume_kafka_events(args.kafka_bootstrap)