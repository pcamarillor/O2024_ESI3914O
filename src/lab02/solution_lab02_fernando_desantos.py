from pyspark.sql import SparkSession
import re

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ITESO-2024-SparkIntroduction") \
    .config("spark.driver.bindAddress", "localhost") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

sc = spark.sparkContext

def analyze_log(log_rdd):
    # Expresion regular para identificar una direccion
    # IPV4 al inicio de cada linea en los logs
    ip_pattern = r'^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
    
    # Filtramos posibles lineas en las que no se encuentre una direccion
    # IP con re.match(ip_pattern, line) y match: match is not None
    # Tomamos el primer match de una direccion -> (match.group(1))
    # le agregamos un 1 en cada tupla -> ((ip,1))
    # agrupamos por cada direccion y sumamos el valor con reduceByKey
    ip_counts = log_rdd \
        .map(lambda line: re.match(ip_pattern, line)) \
        .filter(lambda match: match is not None) \
        .map(lambda match: match.group(1)) \
        .map(lambda ip: (ip, 1)) \
        .reduceByKey(lambda a, b: a + b)
    
    result = ip_counts.collectAsMap()

    return result

log_rdd = sc.textFile("datasets/access.log")

result = analyze_log(log_rdd)

print(result)

sc.stop()