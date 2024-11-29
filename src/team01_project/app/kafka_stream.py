from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Esquema de los datos
schema = StructType([
    StructField("source_ip", StringType(), True),
    StructField("destination_ip", StringType(), True),
    StructField("source_port", IntegerType(), True),
    StructField("destination_port", IntegerType(), True),
    StructField("protocol", StringType(), True),
    StructField("log_time", TimestampType(), True),
    StructField("packet_size", IntegerType(), True),
    StructField("action", StringType(), True)
])

def read_kafka_stream(spark, kafka_broker="localhost:9092", topic="raw_traffic_logs"):
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

    # Deserializar JSON desde Kafka
    traffic_logs = kafka_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    return traffic_logs
