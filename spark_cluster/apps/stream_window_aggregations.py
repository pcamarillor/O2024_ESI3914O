import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, explode, split

def consume_kafka_events(kafka_server):
    # Initialize SparkSession
    spark = SparkSession.builder \
                .appName("Structured-Streaming-Kafka-Example") \
                .config("spark.ui.port","4040") \
                .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    # Create DataFrame representing the stream of input studens from file
    kafka_bootstrap_server = "{0}:9093".format(kafka_server)
    print("Establishing connection with {0}".format(kafka_bootstrap_server))
    kafka_df = spark \
        .readStream\
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
        .option("subscribe", "pyspark-example") \
        .load()

    kafka_df.printSchema()

    # Transform binary data to string
    df_input = kafka_df.selectExpr("CAST(value AS STRING)", "timestamp")

    words = df_input.select(explode(split(df_input.value, " ")).alias("word"), "timestamp")
    words.printSchema()

    # The watermark allows late data to update the state within 2 minutes.
    # Late data beyond the 2-minute threshold will be dropped.
    windowed_counts = words \
            .withWatermark("timestamp", "2 minutes") \
            .groupBy(window(words.timestamp,
                            "30 seconds", # Window duration 
                            "5 seconds"), # Slide duration 
                    words.word) \
            .count()
    
    # Perform a word count within fixed-time windows
    query = windowed_counts \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start() \
        .awaitTermination(90)

    print("stream closed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Kafka arguments")
    parser.add_argument('--kafka-bootstrap', required=True, help="Kafka bootstrap server")
    
    args = parser.parse_args()

    consume_kafka_events(args.kafka_bootstrap)
