from pyspark.sql import SparkSession

# Initialize SparkContext
spark = SparkSession.builder \
    .appName("Lecture-06-ReduceByKey") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "localhost") \
    .config("spark.ui.port","4040") \
    .getOrCreate()

documents = [
        (1, "Spark is an open-source distributed computing system"),
        (2, "MapReduce is a programming model for large-scale data processing"),
        (3, "Spark provides an API for distributed data processing")
    ]

sc = spark.sparkContext

#RDD
rdd = sc.parallelize(documents)

words_rdd = rdd.flatMap(lambda tup: [(word, tup[0]) for word in tup[1].split()])

inverted_index_rdd = words_rdd.distinct().groupByKey().mapValues(list)

sorted_result = inverted_index_rdd.sortByKey().collect()

for word, tup_id in sorted_result:
    print(f"{word}: {tup_id}")

# Detener la sesión de Spark
spark.stop()
