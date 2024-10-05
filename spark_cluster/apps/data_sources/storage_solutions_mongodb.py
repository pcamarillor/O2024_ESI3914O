from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
            .appName("Solution-Storage-Examples-MongoDB") \
            .config("spark.ui.port","4040") \
            .config("spark.mongodb.read.connection.uri", "mongodb://mongo-iteso/test.flights") \
            .config("spark.mongodb.write.connection.uri", "mongodb://mongo-iteso/test.flights") \
            .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read the international flights dataset (5M)
df_flights = spark.read \
            .option("inferSchema", "true") \
            .option("header", "true") \
            .csv("/opt/spark-data/international-flights-sql-excercise_5M.csv.gz")

# Write to MongoDB collection
df_flights.write \
    .format("mongodb") \
    .option("database", "test") \
    .option("collection", "flights") \
    .mode("overwrite") \
    .save()

print("done")






