from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, IntegerType, StructField

# Initialize Spark session
spark = SparkSession.builder.appName("ExamplePipeline").getOrCreate()

# Sample data as a collection (list of tuples)
data = [("John", 28, "Engineer"), 
        ("Jane", 35, "Manager"), 
        ("Doe", 22, "Analyst"), 
        ("Alice", 29, "Engineer"), 
        ("Bob", 40, "Manager")]

# Create a DataFrame from the collection
schema = StructType([
    StructField("name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Position", StringType(), True)
])
df = spark.createDataFrame(data, schema)
df.show()

# Pipeline of transformations
# Transformation 1. Filter by Age > 30
filtered_df = df.filter(col("Age") > 30)

# Transformation 2. Group by Occupation and count occurrences
grouped_df = filtered_df.groupBy("Position").count()

# Action: Collect the result and print it
result = grouped_df.show()
    
# Stop the Spark session
spark.stop()
