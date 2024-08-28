from pyspark.sql import SparkSession

# Initialize a SparkSession with the specified Spark master
# See for more details: https://spark.apache.org/docs/latest/submitting-applications.html#master-urls

spark = SparkSession.builder \
    .appName("ITESO-BigData-Lecture-05") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "localhost") \
    .getOrCreate()

# Verify the SparkSession is connected to the correct master
print("Spark Master URL:", spark.sparkContext.master)

# Example: Create a simple DataFrame and show it
data = [("Alice", 34), ("Bob", 45), ("Catherine", 29), ("Daniel", 40)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, schema=columns)
df.show()

# Stop the SparkSession when done
spark.stop()
