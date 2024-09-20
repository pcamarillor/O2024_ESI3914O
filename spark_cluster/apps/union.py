from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Example-Union") \
    .config("spark.ui.port","4040") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Create sample DataFrames
df_a = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df_b = spark.createDataFrame([(3, "Charlie"), (4, "David")], ["id", "name"])
result = df_a.union(df_b)

df_a.show()

df_b.show()

result.show()

import time
time.sleep(180)