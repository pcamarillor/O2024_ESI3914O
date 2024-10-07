from pyspark.sql import SparkSession
import time

# Initialize SparkSession
spark = SparkSession.builder \
            .appName("Structured-Streaming-Rate-Example") \
            .config("spark.ui.port","4040") \
            .config("spark.driver.bindAddress", "localhost") \
            .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

rate_df = spark.readStream \
            .format("rate") \
            .load()

rate_df.printSchema()

query = rate_df.filter(rate_df.value % 2 == 0) \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .start()

time.sleep(10)
query.stop()