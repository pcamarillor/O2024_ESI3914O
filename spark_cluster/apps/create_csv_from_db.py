from pyspark.sql import SparkSession

# Crear la sesión de Spark
spark = SparkSession.builder \
        .appName("Solution-GPS-Storage-MongoDB") \
        .config("spark.ui.port","4040") \
        .config("spark.mongodb.read.connection.uri", "mongodb://mongo-gps/project.gps") \
        .config("spark.mongodb.write.connection.uri", "mongodb://mongo-gps/project.gps") \
        .getOrCreate()

# Leer datos desde MongoDB
df = spark.read.format("mongodb") \
        .option("database", "project") \
        .option("collection", "gps") \
        .load()

# Mostrar los datos (opcional)
df.show()

# Guardar como CSV (sin encabezado por defecto)
df.write.mode("overwrite") \
        .csv("/opt/spark-data/output_csv/", header=True) 
    

print(f"Datos exportados exitosamente a {"/opt/spark-data/output_csv/"}")

# Finalizar la sesión de Spark
spark.stop()
