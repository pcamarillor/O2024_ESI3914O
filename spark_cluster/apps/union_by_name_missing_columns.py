from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Example-Union") \
    .config("spark.ui.port","4040") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Create sample DataFrames
df_a = spark.createDataFrame([(1, "Alice", "NY")], ["id", "name", "city"])
df_b = spark.createDataFrame([(2, "Bob")], ["id", "name"])
result = df_a.unionByName(df_b, allowMissingColumns=True)

df_a.show()
df_b.show()

result.show()

import time
time.sleep(180)