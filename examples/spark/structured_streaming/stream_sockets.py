from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# Initialize SparkSession
spark = SparkSession.builder \
            .appName("Structured-Streaming-Sockets-Example") \
            .config("spark.ui.port","4040") \
            .config("spark.driver.bindAddress", "localhost") \
            .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "5")

# Open the connection to the running socket
lines = spark.readStream \
          .format("socket") \
          .option("host", "localhost") \
          .option("port", 9999) \
          .load()

words = lines.select(explode(split(lines.value, " ")).alias("word"))
wordCounts = words.groupBy("word").count()

query = wordCounts \
    .writeStream \
    .trigger(processingTime='20 seconds') \
    .outputMode("complete") \
    .format("console") \
    .start()
query.awaitTermination()