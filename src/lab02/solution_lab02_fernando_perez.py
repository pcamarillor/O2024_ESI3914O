from pyspark.sql import SparkSession
spark = SparkSession.builder \
  .master("local[*]") \
  .appName("ITESO-2024-SparkIntroduction") \
  .config("spark.driver.bindAddress","localhost") \
  .config("spark.ui.port","4040") \
  .getOrCreate()

sc = spark.sparkContext

dataset = "datasets/access.log"
dataset_rdd = sc.textFile(dataset)

def analyze_log(dataset_rdd):
    # Write your solution here
    
    # Extraer primer string de cada linea
    ip_rdd = dataset_rdd.map(lambda line: line.split(" ")[0])
    # verificamos que empiece con un numero
    ip_rdd = ip_rdd.filter(lambda ip: ip and ip[0].isdigit())

    ip_counts_rdd = ip_rdd.map(lambda ip: (ip, 1)).reduceByKey(lambda a, b: a + b)

    resultado = ip_counts_rdd.collectAsMap()

    print(resultado)

    return resultado


analyze_log(dataset_rdd)

sc.stop()