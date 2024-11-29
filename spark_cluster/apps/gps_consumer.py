import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, explode, split, from_json, col
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StringType, TimestampType, DoubleType, StructField, IntegerType
from pyspark.sql.streaming import StreamingQueryListener

estados_mexico = {
    "Aguascalientes": {"lat_min": 21.88, "lat_max": 22.35, "lon_min": -103.78, "lon_max": -102.21},
    "Baja California": {"lat_min": 32.32, "lat_max": 32.72, "lon_min": -116.63, "lon_max": -113.38},
    "Baja California Sur": {"lat_min": 24.36, "lat_max": 27.11, "lon_min": -114.46, "lon_max": -109.95},
    "Campeche": {"lat_min": 18.00, "lat_max": 19.83, "lon_min": -92.61, "lon_max": -89.18},
    "Chiapas": {"lat_min": 14.60, "lat_max": 17.49, "lon_min": -93.23, "lon_max": -88.09},
    "Chihuahua": {"lat_min": 26.14, "lat_max": 31.43, "lon_min": -109.46, "lon_max": -103.65},
    "Coahuila": {"lat_min": 25.26, "lat_max": 32.73, "lon_min": -106.37, "lon_max": -100.99},
    "Colima": {"lat_min": 18.89, "lat_max": 19.53, "lon_min": -104.47, "lon_max": -103.65},
    "Durango": {"lat_min": 22.23, "lat_max": 26.77, "lon_min": -106.78, "lon_max": -103.00},
    "Guanajuato": {"lat_min": 20.57, "lat_max": 21.59, "lon_min": -102.94, "lon_max": -100.52},
    "Guerrero": {"lat_min": 16.00, "lat_max": 18.30, "lon_min": -101.80, "lon_max": -98.15},
    "Hidalgo": {"lat_min": 19.51, "lat_max": 21.23, "lon_min": -99.85, "lon_max": -97.25},
    "Jalisco": {"lat_min": 18.15, "lat_max": 21.80, "lon_min": -105.38, "lon_max": -101.10},
    "Mexico City": {"lat_min": 19.26, "lat_max": 19.75, "lon_min": -99.75, "lon_max": -98.90},
    "Michoacán": {"lat_min": 18.13, "lat_max": 20.79, "lon_min": -103.70, "lon_max": -100.21},
    "Morelos": {"lat_min": 18.20, "lat_max": 19.60, "lon_min": -99.80, "lon_max": -98.80},
    "Nayarit": {"lat_min": 21.51, "lat_max": 22.85, "lon_min": -106.49, "lon_max": -104.47},
    "Nuevo León": {"lat_min": 24.30, "lat_max": 26.59, "lon_min": -101.80, "lon_max": -98.10},
    "Oaxaca": {"lat_min": 14.50, "lat_max": 18.14, "lon_min": -98.90, "lon_max": -96.00},
    "Puebla": {"lat_min": 18.03, "lat_max": 19.68, "lon_min": -98.74, "lon_max": -96.65},
    "Querétaro": {"lat_min": 20.57, "lat_max": 21.30, "lon_min": -100.70, "lon_max": -98.60},
    "Quintana Roo": {"lat_min": 18.15, "lat_max": 21.60, "lon_min": -88.16, "lon_max": -86.30},
    "San Luis Potosí": {"lat_min": 21.00, "lat_max": 25.02, "lon_min": -101.75, "lon_max": -98.17},
    "Sinaloa": {"lat_min": 22.10, "lat_max": 26.51, "lon_min": -109.80, "lon_max": -105.35},
    "Sonora": {"lat_min": 28.01, "lat_max": 32.72, "lon_min": -114.87, "lon_max": -108.97},
    "Tabasco": {"lat_min": 17.23, "lat_max": 19.32, "lon_min": -94.88, "lon_max": -91.56},
    "Tamaulipas": {"lat_min": 22.25, "lat_max": 27.00, "lon_min": -99.00, "lon_max": -97.16},
    "Tlaxcala": {"lat_min": 19.10, "lat_max": 19.59, "lon_min": -98.37, "lon_max": -97.38},
    "Veracruz": {"lat_min": 17.02, "lat_max": 21.18, "lon_min": -97.14, "lon_max": -94.50},
    "Yucatán": {"lat_min": 20.70, "lat_max": 21.90, "lon_min": -89.65, "lon_max": -87.60},
    "Zacatecas": {"lat_min": 22.00, "lat_max": 26.40, "lon_min": -104.20, "lon_max": -101.50}
}


# Función para obtener el estado basado en latitud y longitud
def get_estado(latitude, longitude):
    for estado, coords in estados_mexico.items():
        if coords["lat_min"] <= latitude <= coords["lat_max"] and coords["lon_min"] <= longitude <= coords["lon_max"]:
            return estado
    return "Desconocido"  # Si no se encuentra dentro de ningún estado conocido

class MyStreamingQueryListener(StreamingQueryListener):

    def onQueryStarted(self, event):
        print(f"Query started: {event.id}")

    def onQueryProgress(self, event):
        rows_per_second = event.progress.processedRowsPerSecond
        if rows_per_second:
            print(f"Batch ID: {event.progress.batchId}, NumInputRows:{event.progress.numInputRows}, Proccessed Rows Per second: {rows_per_second}")
    
    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")


def consume_kafka_events(kafka_server):
    # Initialize SparkSession
    spark = SparkSession.builder \
                .appName("Structured-Streaming-Sensor-Example") \
                .config("spark.ui.port","4040") \
                .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    # Create DataFrame representing the stream of input studens from file
    kafka_bootstrap_server = "{0}:9093".format(kafka_server)
    print("Establishing connection with {0}".format(kafka_bootstrap_server))

    kafka_df = spark \
        .readStream\
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
        .option("subscribe", "gps-topic-location,gps-topic-location2,gps-topic-location3") \
        .option("startingOffsets", "latest") \
        .load()

    # Define the schema of the incoming JSON data
    gps_schema = StructType([StructField("latitude", DoubleType(), True),
                                StructField("longitude", DoubleType(), True),
                                StructField("timestamp", StringType(), True),
                                StructField("speed", DoubleType(), True)
    ])

    # Crea una UDF para mapear la latitud y longitud al estado
    get_estado_udf = udf(get_estado, StringType())

    # Convierte los datos de Kafka de binario a string
    gps_data_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")

    # Parsear el JSON con el esquema definido
    gps_data_df = gps_data_df.withColumn("gps_location", from_json(gps_data_df.json_value, gps_schema))

    # Añadir la columna "location" usando la UDF
    gps_data_df = gps_data_df.withColumn("state", get_estado_udf(gps_data_df["gps_location.latitude"], gps_data_df["gps_location.longitude"]))
    
    gps_data_df = gps_data_df.filter((gps_data_df["state"] != "Desconocido"))

    # Seleccionamos las columnas de interés
    gps_data_df = gps_data_df.select("gps_location.*", "state")

    # Escribir el DataFrame de streaming a la consola
    """query_console = gps_data_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()"""

    # Escribir el DataFrame de streaming en archivos Parquet
    query_parquet = gps_data_df \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "/opt/spark-data/parquet_files/output/") \
        .option("checkpointLocation", "/opt/spark-data/parquet_files/checkpoint/") \
        .trigger(processingTime='1 seconds') \
        .start()
    
    # Add listener
    spark.streams.addListener(MyStreamingQueryListener())

    # Esperar a que termine el streaming
    #query_console.awaitTermination(10)
    query_parquet.awaitTermination(60)

    print("stream closed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Kafka arguments")
    parser.add_argument('--kafka-bootstrap', required=True, help="Kafka bootstrap server")
    
    args = parser.parse_args()

    consume_kafka_events(args.kafka_bootstrap)

