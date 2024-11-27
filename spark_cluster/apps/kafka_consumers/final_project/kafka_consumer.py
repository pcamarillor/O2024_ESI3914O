import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, StructField, IntegerType
from pyspark.sql.streaming import StreamingQueryListener


class PerformanceListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Query started: {event.id}")

    def onQueryProgress(self, event):
        print(
            f"Batch ID: {event.progress.batchId}, NumInputRows:{event.progress.numInputRows}, Processed Rows Per second: {event.progress.processedRowsPerSecond}"
        )

    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")


# Add listener


def consume_kafka_events(kafka_server="e30cf11ddcb6"):
    # Initialize SparkSession
    spark = (
        SparkSession.builder.appName("Structured-Streaming-Kafka-Example")
        .config("spark.ui.port", "4040")
        .getOrCreate()
    )

    spark.streams.addListener(PerformanceListener())
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    # Define the schema for each topic
    tweet_schema = StructType(
        [
            StructField("content", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("user_id", IntegerType(), True),
        ]
    )

    repost_schema = StructType(
        [
            StructField("original_post_id", IntegerType(), True),
            StructField("comment", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("user_id", IntegerType(), True),
        ]
    )

    like_schema = StructType(
        [
            StructField("post_id", IntegerType(), True),
            StructField("event_time", StringType(), True),
            StructField("user_id", IntegerType(), True),
        ]
    )

    comment_schema = StructType(
        [
            StructField("post_id", IntegerType(), True),
            StructField("text", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("user_id", IntegerType(), True),
        ]
    )

    # Kafka bootstrap server
    kafka_bootstrap_server = f"{kafka_server}:9093"
    print(f"Establishing connection with {kafka_bootstrap_server}")

    # Subscribe to topics
    topics = ["tweet", "repost", "like", "comment"]
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_server)
        .option("subscribe", ",".join(topics))
        .option("startingOffsets", "latest")
        .load()
    )

    kafka_df.printSchema()

    # Transform binary data to string
    kafka_data_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value", "topic")

    # Process each topic
    def process_topic(topic_name, schema, output_path):
        df = (
            kafka_data_df.filter(col("topic") == topic_name)
            .withColumn("data", from_json(col("json_value"), schema))
            .select("data.*")
        )

        # Add a print transformation to log each record found
        query = (
            df.writeStream.outputMode("append")
            .format("console")  # Print to console
            .option("truncate", "false")
            .start()
        )

        # Write each topic to a parquet file
        df.writeStream.outputMode("append").format("parquet").option(
            "path", "/opt/spark-data/final_project/" + output_path
        ).option("checkpointLocation", f"{output_path}_checkpoint").start()

        return query

    tweet_df = process_topic("tweet", tweet_schema, "tweets_output")
    repost_df = process_topic("repost", repost_schema, "reposts_output")
    like_df = process_topic("like", like_schema, "likes_output")
    comment_df = process_topic("comment", comment_schema, "comments_output")

    # Await termination (keeps all streams active)
    spark.streams.awaitAnyTermination()

    print("Streams closed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Kafka arguments")
    parser.add_argument(
        "--kafka-bootstrap", required=True, help="Kafka bootstrap server"
    )

    args = parser.parse_args()

    consume_kafka_events(args.kafka_bootstrap)
