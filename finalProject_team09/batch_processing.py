# batch_processing.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, expr
from pyspark.sql.types import IntegerType, FloatType
import sys

def main():
    try:
        # Inicializar la sesión de Spark con el driver JDBC de PostgreSQL
        spark = SparkSession.builder \
            .appName("BatchProcessingPostgreSQL") \
            .config("spark.jars", "/Users/franciscofloresenriquez/O2024_ESI3914O/finalProject_team09/postgresql-42.7.4.jar") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("INFO")

        # Leer datos desde un directorio Parquet
        input_path_clicks = "/tmp/parquet_data_clicks"
        input_path_views = "/tmp/parquet_data_views"

        clicks_df = spark.read.parquet(input_path_clicks)
        views_df = spark.read.parquet(input_path_views)

        # Mostrar el esquema original
        print("Esquema original de clicks_df:")
        clicks_df.printSchema()

        # Transformaciones en el DataFrame
        filtered_clicks = clicks_df.filter(col("event_type") == "click")
        filtered_views = views_df.filter(col("event_type") == "view")

        # Añadir la columna 'click_id' como UUID
        filtered_clicks = filtered_clicks.withColumn("click_id", expr("uuid()"))

        # Añadir la columna 'view_id' como UUID
        filtered_views = filtered_views.withColumn("view_id", expr("uuid()"))

        # Convertir 'timestamp' a TimestampType
        filtered_clicks = filtered_clicks.withColumn("timestamp", to_timestamp(col("timestamp")))
        filtered_views = filtered_views.withColumn("timestamp", to_timestamp(col("timestamp")))

        # Convertir 'user_id' y 'click_count' a IntegerType
        filtered_clicks = filtered_clicks.withColumn("user_id", col("user_id").cast(IntegerType()))
        filtered_clicks = filtered_clicks.withColumn("click_count", col("click_count").cast(IntegerType()))

        # Convertir 'user_id' y 'view_duration' en 'views' a los tipos correctos
        filtered_views = filtered_views.withColumn("user_id", col("user_id").cast(IntegerType()))
        filtered_views = filtered_views.withColumn("view_duration", col("view_duration").cast(FloatType()))

        # Mostrar los esquemas transformados
        print("Esquema transformado de filtered_clicks:")
        filtered_clicks.printSchema()

        print("Esquema transformado de filtered_views:")
        filtered_views.printSchema()

        # Definir las propiedades de conexión JDBC
        jdbc_url = "jdbc:postgresql://localhost:5433/webActivity?stringtype=unspecified"
        jdbc_properties = {
            "user": "project",
            "password": "final",
            "driver": "org.postgresql.Driver"
        }

        # Escribir los DataFrames filtrados en PostgreSQL
        filtered_clicks.write \
            .jdbc(url=jdbc_url, table="clicks", mode="append", properties=jdbc_properties)

        filtered_views.write \
            .jdbc(url=jdbc_url, table="views", mode="append", properties=jdbc_properties)

        print("Datos procesados escritos exitosamente en PostgreSQL")

        # Detener la sesión de Spark
        spark.stop()

    except Exception as e:
        print("Ocurrió un error durante el procesamiento:", file=sys.stderr)
        print(str(e), file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()