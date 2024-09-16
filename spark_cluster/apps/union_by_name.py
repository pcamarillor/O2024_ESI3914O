from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Example-Union") \
    .config("spark.ui.port","4040") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Create sample DataFrames
df_a = spark.createDataFrame([(1, "Alice")], ["id", "name"])
df_b = spark.createDataFrame([("Bob", 2)], ["name", "id"])
result = df_b.unionByName(df_a)

df_a.show()
df_b.show()

result.show()

import time
time.sleep(180)