from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# Initialize SparkSession
spark = SparkSession.builder \
            .appName("Structured-Streaming-Files-Example") \
            .config("spark.ui.port","4040") \
            .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "5")

# Define the schema
schema = StructType([
    StructField("student_id", IntegerType(), True),
    StructField("student_first_name", StringType(), True),
    StructField("student_last_name", StringType(), True),
    StructField("gpa", FloatType(), True)
])

students_df = spark.readStream \
            .format("json") \
            .schema(schema) \
            .load("/opt/spark-data/streaming_examples/")

students_df = students_df.filter(students_df.gpa > 3)

query = students_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .trigger(processingTime='3 seconds') \
        .start()

query.awaitTermination(20)
