from pyspark.sql import SparkSession

# Initialize SparkContext
spark = SparkSession.builder \
    .appName("Lecture-06-ReduceByKey") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "localhost") \
    .config("spark.ui.port","4040") \
    .getOrCreate()

sc = spark.sparkContext

log_file_path = r"C:\Users\uriba\OneDrive\Desktop\Datos Masivos\O2024_ESI3914O\datasets\access.log"

log_rdd = sc.textFile(log_file_path)


def analyze_log(log_rdd):
    ip_count_dict = dict(
        log_rdd.map(lambda line: line.split(" ")[0])  # Extraer la IP de cada línea
              .map(lambda ip: (ip, 1))                # Crear pares (IP, 1)
              .reduceByKey(lambda a, b: a + b)        # Contar las solicitudes por IP
              .collect()                              # Recoger los resultados como una lista de tuplas
    )
    return ip_count_dict


result = analyze_log(log_rdd)
print(result)


sc.stop()