#from pyspark.sql import SparkSession

#spark = SparkSession.builder \
#    .appName("Lecture-06-GroupBy") \
#    .master("local[*]") \
#    .config("spark.driver.bindAddress", "localhost") \
#    .config("spark.ui.port","4040") \
#    .getOrCreate()

#sc = spark.sparkContext
#sc.setLogLevel("ERROR")

def analyze_log(log_rdd):
    #rdd = sc.parallelize(log_rdd)
    log_line = log_rdd.filter(lambda line : line.strip() != "")
    ips_count = log_line.map(lambda log : (log.split()[0], 1)).reduceByKey(lambda a, b : a + b)
    #print(f'Logs: {dict(logs.collect())}')
    return dict(ips_count.collect())


#sc.stop()

