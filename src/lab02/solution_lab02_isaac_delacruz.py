from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .master("local[*]") \
  .appName("ITESO-2024-SparkIntroduction") \
  .config("spark.driver.bindAddress","localhost") \
  .config("spark.ui.port","4040") \
  .getOrCreate()

sc = spark.sparkContext

log_file = open("./../../datasets/access.log")
log_data = log_file.read().splitlines()

log_rdd = sc.parallelize(log_data)

def analyze_log(log_rdd):
    ip_count = log_rdd.map(lambda line: (line.split(" ")[0], 1)).reduceByKey(lambda a, b: a + b)
    return ip_count.collectAsMap()

print(analyze_log(log_rdd))