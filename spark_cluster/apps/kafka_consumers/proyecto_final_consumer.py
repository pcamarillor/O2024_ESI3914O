from pyspark.sql.streaming import StreamingQueryListener
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, BooleanType, StructField

class CustomStreamingQueryListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Query started: {event.id}, {event.name}")

    def onQueryProgress(self, event):
        # Processed rows per second
        rows_per_second = event.progress.processedRowsPerSecond
        if rows_per_second:
            print(f"Batch ID: {event.progress.batchId}, NumInputRows:{event.progress.numInputRows}")
            print(f"Processed Rows Per Second: {rows_per_second}")
        else:
            print("No processedRowsPerSecond information available")

    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")

def consume_kafka_events(kafka_server):
    # Initialize SparkSession
    spark = SparkSession.builder \
                .appName("Network-Traffic-Monitoring") \
                .config("spark.ui.port", "4040") \
                .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    # Register custom query listener
    listener = CustomStreamingQueryListener()
    spark.streams.addListener(listener)

    # Define the schema of the incoming JSON data
    traffic_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("source_ip", StringType(), True),
        StructField("destination_ip", StringType(), True),
        StructField("source_port", IntegerType(), True),
        StructField("destination_port", IntegerType(), True),
        StructField("protocol", StringType(), True),
        StructField("packet_size_bytes", IntegerType(), True),
        StructField("flow_id", StringType(), True),
        StructField("traffic_type", StringType(), True),
        StructField("alerts", StructType([
            StructField("malicious_activity", BooleanType(), True),
            StructField("unusual_pattern", BooleanType(), True)
        ]))
    ])

    # Kafka bootstrap server
    kafka_bootstrap_server = f"{kafka_server}:9093"
    print(f"Establishing connection with {kafka_bootstrap_server}")
    
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
        .option("subscribe", "network-traffic,network-traffic-2,network-traffic-3") \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", 5000) \
        .load()

    # Transform binary data to string
    traffic_data_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")

    # Parse the JSON string into separate columns based on the schema
    traffic_data_df = traffic_data_df.withColumn("traffic_data", from_json(traffic_data_df.json_value, traffic_schema))

    # Select the parsed fields
    traffic_data_df = traffic_data_df.select("traffic_data.*")

    # Convert the timestamp to a TimestampType for time-based operations
    traffic_data_df = traffic_data_df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

    # Persist the raw data to Parquet
    raw_data_query = traffic_data_df.writeStream \
        .format("parquet") \
        .option("path", "/opt/spark-data/final/parquets") \
        .option("checkpointLocation", "/opt/spark-data/final/checkpoint") \
        .outputMode("append") \
        .start()

    # Await termination to keep the streaming query running
    raw_data_query.awaitTermination()

    print("Stream closed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Kafka arguments")
    parser.add_argument('--kafka-bootstrap', required=True, help="Kafka bootstrap server")
    
    args = parser.parse_args()

    consume_kafka_events(args.kafka_bootstrap)
