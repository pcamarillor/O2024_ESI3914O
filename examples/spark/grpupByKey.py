from pyspark.sql import SparkSession 

# Initialize SparkContext
spark = SparkSession.builder \
    .appName("Lecture-06-GroupBy") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "localhost") \
    .config("spark.ui.port","4040") \
    .getOrCreate()

sc = spark.sparkContext

# Step 1: Create an RDD with key-value pairs
data = [
    ('fruit', 'apple'),
    ('fruit', 'banana'),
    ('vegetable', 'carrot'),
    ('fruit', 'orange'),
    ('vegetable', 'broccoli')
]

# Create an RDD from the list of key-value pairs
rdd = sc.parallelize(data)

# Step 2: Use groupByKey to group values by key
grouped_rdd = rdd.groupByKey()

# Step 3: Collect and print the results
result = grouped_rdd.collect()
for key, values in result:
    print(f"{key}: {list(values)}")

# Stop the SparkContext
sc.stop()
