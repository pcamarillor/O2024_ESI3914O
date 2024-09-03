from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("DF Sales Example").getOrCreate()

df = spark.createDataFrame([
    (1, "North", 1000),
    (2, "South", 2000),
    (3, "North", 1500)
], ["id", "region", "sales"])

df.show()
df.printSchema()

sum_sales_df = df.groupBy("region").sum("sales")

sum_sales_df.printSchema()
sum_sales_df.show()

spark.sparkContext.stop()