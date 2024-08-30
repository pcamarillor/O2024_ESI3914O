from pyspark.sql import SparkSession

# Initialize SparkContext
spark = SparkSession.builder \
    .appName("Lecture-06-ReduceByKey") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "localhost") \
    .config("spark.ui.port","4040") \
    .getOrCreate()

# Step 1: Create an RDD with key-value pairs
data = [
    ('apple', 1),
    ('banana', 2),
    ('apple', 3),
    ('orange', 4),
    ('banana', 1),
    ('orange', 2)
]
sc = spark.sparkContext
# Create an RDD from the list of key-value pairs
rdd = sc.parallelize(data)

# Step 2: Use reduceByKey to sum values for each key
reduced_rdd = rdd.reduceByKey(lambda a, b: a + b)

# Step 3: Collect and print the results
result = reduced_rdd.collect()
for key, value in result:
    print(f"{key}: {value}")

# Stop the SparkContext
    sc.stop()
