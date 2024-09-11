from pyspark.sql import SparkSession

# Create Spark Session in localhost
spark = SparkSession.builder.master("local") \
    .appName("Transformations example: Even Numbers") \
    .getOrCreate()

# Create Spark Context
sc = spark.sparkContext

# Create a data frame with integers from 0 to 1000
my_range = spark.range(1000).toDF("number")

# Generate a new DF from the result of a tranformation
even_numbers = my_range.filter(my_range["number"] % 2 == 0)
even_numbers.show(n=10)

sc.stop()