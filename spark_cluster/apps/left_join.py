from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize SparkSession
spark = SparkSession.builder \
            .appName("Example-Left-Join") \
            .config("spark.ui.port","4040") \
            .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define schema for Book
book_schema = StructType([
    StructField("book_name", StringType(), True),
    StructField("cost", IntegerType(), True),
    StructField("writer_id", IntegerType(), True)
])

# Create Book DataFrame
book_data = [
    ("Scala", 400, 1),
    ("Spark", 500, 2),
    ("Kafka", 300, 3),
    ("Java", 350, 5)
]

df_books = spark.createDataFrame(book_data, schema=book_schema)
df_books.show()

# Define schema for Writer
writer_schema = StructType([
    StructField("writer_name", StringType(), True),
    StructField("writer_id", IntegerType(), True)
])

# Create Writer DataFrame
writer_data = [
    ("Martin", 1),
    ("Zaharia", 2),
    ("Neha", 3),
    ("James", 4)
]

df_writers = spark.createDataFrame(writer_data, schema=writer_schema)
df_writers.show()

result = df_books.join(df_writers, 
      df_books["writer_id"] == df_writers["writer_id"], "left")

result = df_books.join(df_writers, on="writer_id", how="left")

result = result.select("book_name", "cost", "writer_name").fillna({"writer_name": "Unknown"})

result.show()