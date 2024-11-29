import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, StructField

def consume_network_traffic(kafka_server, kafka_topic):
    spark = SparkSession.builder \
        .appName("Network-Traffic-Streaming") \
        .config("spark.ui.port", "4040") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("source_ip", StringType(), True),
        StructField("destination_ip", StringType(), True),
        StructField("protocol", StringType(), True),
        StructField("source_port", IntegerType(), True),
        StructField("destination_port", IntegerType(), True),
        StructField("data_transferred", IntegerType(), True),
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .load()

    kafka_data_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")
    network_traffic_df = kafka_data_df.select(from_json(col("json_value"), schema).alias("data")).select("data.*")

    query = network_traffic_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "/opt/spark-data/network_traffic_data/") \
        .option("checkpointLocation", "/opt/spark-data/network_traffic_checkpoint/") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consume network traffic events from Kafka")
    parser.add_argument("--kafka-bootstrap", required=True, help="Kafka server")
    parser.add_argument("--kafka-topic", required=True, help="Kafka topic to consume")
    args = parser.parse_args()

    consume_network_traffic(args.kafka_bootstrap, args.kafka_topic)
