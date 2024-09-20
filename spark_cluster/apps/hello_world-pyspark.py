from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ITESO-BigData-Hello-World-App") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")
# Verify the SparkSession is connected to the correct master
print("Spark Master URL:", sc.master)
print("Hello World App - ITESO - Big Data - 2024")
# Step 1: Create an RDD with a list of words
words = ["apple", "banana", "apple", "orange", "banana", "apple", "orange", "orange"]
words_rdd = sc.parallelize(words)

# Step 2: Use countByValue to count the occurrences of each word
word_counts = words_rdd.countByValue()

# Step 3: Print the results
for word, count in word_counts.items():
    print(f"{word}: {count}")

# Stop the SparkSession when done
spark.stop()
