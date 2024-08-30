from pyspark.sql import SparkSession

# Initialize SparkContext
spark = SparkSession.builder \
    .appName("Lecture-06-Index-Invertor") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "localhost") \
    .config("spark.ui.port","4040") \
    .getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

# Step 1: Read and Prepare Data
# Suppose we have a list of documents, each represented as a tuple (doc_id, text)
documents = [
    (1, "Spark is an open-source distributed computing system"),
    (2, "MapReduce is a programming model for large-scale data processing"),
    (3, "Spark provides an API for distributed data processing")
]

# Create an RDD from the list of documents
doc_rdd = sc.parallelize(documents)

# Step 2: Map Phase
# Create key-value pairs where the key is a word and the value is the document ID
def map_words(doc):
    doc_id, text = doc
    words = text.split()
    return [(word, doc_id) for word in words]

# Use flatMap to flatten the list of tuples into a single RDD of key-value pairs
mapped_rdd = doc_rdd.flatMap(map_words)

# Step 3: Shuffle and Sort Phase
# Group the values by key (word)
grouped_rdd = mapped_rdd.groupByKey()

# Step 4: Reduce Phase
# Convert grouped values into a list
def to_list(iterable):
    return list(iterable)

inverted_index_rdd = grouped_rdd.mapValues(to_list)

# Collect and print the results
inverted_index = inverted_index_rdd.collect()
for word, doc_ids in inverted_index:
    print(f"{word}: \t {doc_ids}")

# Stop the SparkContext
sc.stop()
