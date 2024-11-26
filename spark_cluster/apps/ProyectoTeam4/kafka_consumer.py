import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, explode, split, from_json, col, count, sum, avg
from pyspark.sql.types import StructType, StringType, TimestampType, DoubleType, StructField, IntegerType
from pyspark.sql.streaming import StreamingQueryListener

class QueryProgressListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        pass
    
    def onQueryProgress(self, event):
        progress = event.progress
        print(f"Rows processed per second: {progress.processedRowsPerSecond}")
        print(f"Number of input rows: {progress.numInputRows}")
    
    def onQueryTerminated(self, event):
        pass

def consume_kafka_events(kafka_server):
    # Initialize SparkSession
    spark = SparkSession.builder \
                .appName("Structured-Streaming-Sensor-Example") \
                .config("spark.ui.port","4040") \
                .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    # Define the schema of the incoming JSON data
    video_schema = StructType([StructField("video_id", StringType(), True),
                                StructField("video_title", StringType(), True),
                                StructField("timestamp", StringType(), True),
                                StructField("playback_quality", StringType(), True),
                                StructField("buffering_duration", DoubleType(), True),
                                StructField("engagement_duration", IntegerType(), True),
                                StructField("device_type", StringType(), True),
                                StructField("recommendation_clicked", StringType(), True),
                                StructField("viewer_location", StringType(), True)
    ])

    spark.streams.addListener(QueryProgressListener())

    # Create DataFrame representing the stream of input studens from file
    kafka_bootstrap_server = "{0}:9093".format(kafka_server)
    print("Establishing connection with {0}".format(kafka_bootstrap_server))
    kafka_df = spark \
        .readStream\
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
        .option("subscribe", "IsaacProducer,MiguelProducer,MarceloProducer") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    kafka_df.printSchema()

    # Transform binary data to string
    video_data_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")

    # Parse the JSON string into separate columns based on the schema
    video_data_df = video_data_df.withColumn("video_data", from_json(video_data_df.json_value, video_schema))

    # Select the parsed fields (sensor_id, event_time, temperature)
    video_data_df = video_data_df.select("video_data.*")

    # Convert the event_time to a TimestampType (for time-based operations)
    video_data_df = video_data_df.withColumn("timestamp", video_data_df.timestamp.cast(TimestampType()))
    
    video_data_df.printSchema()

    windowedAggregates = video_data_df.withWatermark("timestamp", "10 minutes") \
                           .groupBy(window(col("timestamp"), "10 minutes", "5 minutes"),
                                           col("video_id")) \
                           .agg(count("video_id"), avg("buffering_duration"))

    # Output the results to the console
    query = windowedAggregates.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "/opt/spark-data/ProyectoTeam4/data") \
        .option("checkpointLocation", "/opt/spark-data/ProyectoTeam4/checkpoints") \
        .start()
    


    # Await termination to keep the streaming query running
    query.awaitTermination(60)

    print("stream closed")

def write_parquet_cassandra_db():
    spark = SparkSession.builder \
                .appName("CassandraTeam4") \
                .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
                .config("spark.cassandra.connection.host", "cassandra-iteso") \
                .config("spark.cassandra.connection.port", "9042") \
                .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    df_parquet = spark.read.parquet("/opt/spark-data/ProyectoTeam4/data")

    df_parquet = df_parquet.withColumn("window_start", df_parquet["window.start"]) \
                            .withColumn("window_end", df_parquet["window.end"]) \
                            .drop("window")

    df_parquet = df_parquet \
        .withColumnRenamed("count(video_id)", "count_video_id") \
        .withColumnRenamed("avg(buffering_duration)", "avg_buffering_duration")

    print("Show me the dataframe out of parquet files")
    df_parquet.show()

    df_parquet.write\
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table = "video_aggregates", keyspace = "team4") \
        .save()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Kafka arguments")
    parser.add_argument('--kafka-bootstrap', required=True, help="Kafka bootstrap server")
    
    args = parser.parse_args()

    consume_kafka_events(args.kafka_bootstrap)
    write_parquet_cassandra_db()

