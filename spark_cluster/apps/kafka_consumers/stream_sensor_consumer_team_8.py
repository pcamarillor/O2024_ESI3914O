import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, from_json, col, avg, sum
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

    # Create DataFrame representing the stream of input students from file
    kafka_bootstrap_server = "{0}:9093".format(kafka_server)
    print("Establishing connection with {0}".format(kafka_bootstrap_server))
    kafka_df = spark \
        .readStream\
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
        .option("subscribe", "pyspark-example") \
        .option("startingOffsets", "latest") \
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

    # Add watermark to allow processing of late events within 5 minutes
    sensor_data_df = sensor_data_df.withWatermark("event_time", "5 minutes")

    # Perform window-based aggregations (average and sum) with 30-second time windows
    avg_temperature_df = sensor_data_df.groupBy(
        window(sensor_data_df.event_time, "30 seconds"),
        sensor_data_df.sensor_id
    ).agg(avg("temperature").alias("avg_temperature"))

    sum_temperature_df = sensor_data_df.groupBy(
        window(sensor_data_df.event_time, "30 seconds"),
        sensor_data_df.sensor_id
    ).agg(sum("temperature").alias("sum_temperature"))

    # Output the average temperature results to the console
    avg_query = avg_temperature_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Output the sum temperature results to the console
    sum_query = sum_temperature_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Await termination to keep the streaming queries running
    avg_query.awaitTermination(20)
    sum_query.awaitTermination(20)

    print("stream closed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Kafka arguments")
    parser.add_argument('--kafka-bootstrap', required=True, help="Kafka bootstrap server")
    
    args = parser.parse_args()

    consume_kafka_events(args.kafka_bootstrap)
