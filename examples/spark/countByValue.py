from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext("local", "CountByValueExample")

# Step 1: Create an RDD with a list of words
words = ["apple", "banana", "apple", "orange", "banana", "apple", "orange", "orange"]
words_rdd = sc.parallelize(words)

# Step 2: Use countByValue to count the occurrences of each word
word_counts = words_rdd.countByValue()

# Step 3: Print the results
for word, count in word_counts.items():
    print(f"{word}: {count}")

# Stop the SparkContext
sc.stop()
