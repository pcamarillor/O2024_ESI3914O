from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()

# Define data schema
schema_clicks = StructType() \
    .add("event_type", StringType()) \
    .add("timestamp", StringType()) \
    .add("user_id", IntegerType()) \
    .add("page_id", StringType()) \
    .add("click_count", IntegerType())

schema_views = StructType() \
    .add("event_type", StringType()) \
    .add("timestamp", StringType()) \
    .add("user_id", IntegerType()) \
    .add("page_id", StringType()) \
    .add("view_duration", FloatType())

# Read from Kafka
df_clicks = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic_user_clicks") \
    .option("failOnDataLoss", "false") \
    .load() \
    .select(from_json(col("value").cast("string"), schema_clicks).alias("data")).select("data.*")

df_views = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic_page_views") \
    .option("failOnDataLoss", "false") \
    .load() \
    .select(from_json(col("value").cast("string"), schema_views).alias("data")).select("data.*")

# Save in Parquet format
query_clicks = df_clicks.writeStream \
    .format("parquet") \
    .option("path", "/tmp/parquet_data_clicks") \
    .option("checkpointLocation", "/tmp/checkpoint_clicks") \
    .start()

query_views = df_views.writeStream \
    .format("parquet") \
    .option("path", "/tmp/parquet_data_views") \
    .option("checkpointLocation", "/tmp/checkpoint_views") \
    .start()

query_clicks.awaitTermination()
query_views.awaitTermination()