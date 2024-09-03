from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import count

# Initialize SparkSession
spark = SparkSession.builder.appName("DF Example").getOrCreate()

df = spark.createDataFrame([
    ("Hello world",),
    ("Hello Spark",),
    ("Hello PySpark",)
], ["text"])

df.show()
df.printSchema()

words_df = df.selectExpr("explode(split(text, ' ')) as word")

words_df.printSchema()
words_df.show()


word_count_df = words_df.groupBy("word").agg(count("word"))

word_count_df.printSchema()
word_count_df.show()

spark.sparkContext.stop()