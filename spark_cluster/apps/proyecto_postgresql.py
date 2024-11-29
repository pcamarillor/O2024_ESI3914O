from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName('InsertParquetToPostgres') \
    .config('spark.jars', '/opt/spark-jdbc/postgresql-42.7.4.jar') \
    .getOrCreate()

# Ruta a los archivos Parquet
parquet_path = '/opt/spark-data/final/parquets'

# Leer todos los archivos Parquet desde la carpeta
df = spark.read.parquet(parquet_path)

# Convertir las columnas source_ip y destination_ip a cadenas compatibles con VARCHAR
if 'source_ip' in df.columns and 'destination_ip' in df.columns:
    df = df.withColumn("source_ip", col("source_ip").cast("string"))
    df = df.withColumn("destination_ip", col("destination_ip").cast("string"))

# Descomponer la columna 'alerts' en dos columnas nuevas
if 'alerts' in df.columns:
    df = df.withColumn("malicious_activity", col("alerts.malicious_activity").cast("boolean"))
    df = df.withColumn("unusual_pattern", col("alerts.unusual_pattern").cast("boolean"))

# Eliminar la columna 'alerts'
df = df.drop("alerts")

# Parámetros de conexión a PostgreSQL
url = 'jdbc:postgresql://postgres_final_project:5432/mimic_db'
properties = {
    'user': 'admin',
    'password': 'mimic_user',
    'driver': 'org.postgresql.Driver'
}

# Escribir el DataFrame a PostgreSQL
df.write.jdbc(
    url=url,
    table='network_logs',  # Asegúrate de que esta tabla exista en la base de datos
    mode='append',  # Inserta los datos en la tabla existente
    properties=properties
)

print("Los datos Parquet han sido insertados en la base de datos.")
