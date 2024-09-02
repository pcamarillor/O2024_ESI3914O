from pyspark.sql import SparkSession
import re

# Configure the Spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ITESO-2024-SparkIntroduction") \
    .config("spark.driver.bindAddress", "localhost") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

sc = spark.sparkContext

def analyze_log(log_rdd):

    # Regular expression pattern to match valid IPv4 addresses
    ip_pattern = r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b'
    
    # Extract IPs using flatMap to directly emit IPs or nothing
    ip_rdd = log_rdd.flatMap(lambda line: re.findall(ip_pattern, line))
    
    # Count the number of requests per IP using map and reduceByKey
    ip_count_rdd = ip_rdd.map(lambda ip: (ip, 1)) \
                         .reduceByKey(lambda a, b: a + b)
    
    result_dict = dict(ip_count_rdd.collect())
    
    return result_dict

# Run analysis
result = analyze_log(log_rdd)

# Print results
for ip, count in result.items():
    print(f"{ip}: {count}")

# Stop the SparkContext
sc.stop()
