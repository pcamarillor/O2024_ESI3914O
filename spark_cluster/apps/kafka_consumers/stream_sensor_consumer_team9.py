import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, explode, split, from_json, col, count, sum, avg, min, max
from pyspark.sql.types import StructType, StringType, TimestampType, DoubleType, StructField

def consume_kafka_events(kafka_server):
    spark = SparkSession.builder \
                .appName("Structured-Streaming-Sensor-Example") \
                .config("spark.ui.port","4040") \
                .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    sensor_schema = StructType([StructField("sensor_id", StringType(), True),
                                StructField("event_time", StringType(), True),
                                StructField("temperature", DoubleType(), True)
    ])
    # Create DataFrame representing the stream of input data from Kafka
    kafka_bootstrap_server = "{0}:9092".format(kafka_server)
    print("Establishing connection with {0}".format(kafka_bootstrap_server))
    kafka_df = spark \
        .readStream\
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
        .option("subscribe", "pyspark-example") \
        .option("startingOffsets",
                "latest") \
        .load()

    kafka_df.printSchema()

    sensor_data_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")

    sensor_data_df = sensor_data_df.withColumn("sensor_data", from_json(sensor_data_df.json_value, sensor_schema))

    sensor_data_df = sensor_data_df.select("sensor_data.*")

    sensor_data_df = sensor_data_df.withColumn("event_time", sensor_data_df.event_time.cast(TimestampType()))

    windowedAggregates = sensor_data_df.withWatermark("event_time", "5 minutes") \
                           .groupBy(window(col("event_time"), "2 minutes", "30 seconds"),
                                    col("sensor_id")) \
                           .agg(count("sensor_id").alias("reading_count"),
                                avg("temperature").alias("avg_temperature"),
                                sum("temperature").alias("sum_temperature"),
                                min("temperature").alias("min_temperature"),
                                max("temperature").alias("max_temperature"))

    # Output the results to the console
    query = windowedAggregates.writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    query.awaitTermination(20)

    print("stream closed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Kafka arguments")
    parser.add_argument('--kafka-bootstrap', required=True, help="Kafka bootstrap server")

    args = parser.parse_args()

    consume_kafka_events(args.kafka_bootstrap)

    consume_kafka_events(args.kafka_bootstrap)
