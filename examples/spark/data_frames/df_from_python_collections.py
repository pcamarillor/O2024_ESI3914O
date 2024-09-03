from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Initialize SparkSession
spark = SparkSession.builder.appName("DF Example").getOrCreate()

# Sample data
data = [("Alice", 29), ("Bob", 31), ("Cathy", 23)]

# Define schema
schema = StructType([StructField("Name", StringType(), True),
                     StructField("Age", IntegerType(), True)])

# Create DataFrame
df = spark.createDataFrame(data, schema)
df.printSchema()
df.show()

spark.sparkContext.stop()