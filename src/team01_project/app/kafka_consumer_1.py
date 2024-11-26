import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, DoubleType

def consume_kafka_events(kafka_server):
    """
    Función principal para consumir datos de un tópico Kafka,
    procesar los datos de tráfico de red y detectar anomalías.
    
    Args:
        kafka_server (str): Dirección del servidor Kafka (bootstrap server).
    """

    # Inicializar una sesión de Spark
    spark = (
        SparkSession.builder.appName("Network-Traffic-Monitoring")  # Nombre de la aplicación
        .config("spark.ui.port", "4040")  # Configurar el puerto para la interfaz de usuario de Spark
        .getOrCreate()
    )

    # Configuración adicional para Spark
    spark.sparkContext.setLogLevel("ERROR")  # Reducir los logs al nivel de error
    spark.conf.set("spark.sql.shuffle.partitions", "5")  # Ajustar particiones para eficiencia

    # Definir el esquema para los datos de tráfico de red
    traffic_schema = StructType([
        StructField("source_ip", StringType(), True),           # Dirección IP de origen
        StructField("destination_ip", StringType(), True),      # Dirección IP de destino
        StructField("source_port", IntegerType(), True),        # Puerto de origen
        StructField("destination_port", IntegerType(), True),   # Puerto de destino
        StructField("protocol", StringType(), True),            # Protocolo utilizado (TCP, UDP, etc.)
        StructField("bytes_sent", DoubleType(), True),          # Número de bytes enviados
        StructField("event_time", StringType(), True),          # Tiempo del evento
    ])

    # Configuración del servidor Kafka y tópico
    kafka_bootstrap_server = f"{kafka_server}:9093"  # Construcción de la dirección del servidor Kafka
    topic = "network_traffic"  # Nombre del tópico que contiene los datos
    print(f"Connecting to Kafka at {kafka_bootstrap_server}, topic: {topic}")

    # Leer datos del tópico Kafka como un flujo
    kafka_df = (
        spark.readStream.format("kafka")  # Formato de entrada: Kafka
        .option("kafka.bootstrap.servers", kafka_bootstrap_server)  # Dirección del servidor Kafka
        .option("subscribe", topic)  # Nombre del tópico
        .option("startingOffsets", "latest")  # Leer solo los mensajes nuevos (offset más reciente)
        .load()
    )

    # Mostrar el esquema del DataFrame leído de Kafka
    kafka_df.printSchema()

    # Transformar los datos binarios a cadenas JSON para procesamiento
    kafka_data_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")

    # Procesar los datos de tráfico de red
    df = (
        kafka_data_df
        .withColumn("data", from_json(col("json_value"), traffic_schema))  # Convertir JSON a columnas estructuradas
        .select("data.*")  # Seleccionar solo los campos deserializados
    )

    # Detectar anomalías en tiempo real
    # Ejemplo: Filtrar paquetes con un tamaño de más de 1 MB
    anomalies_df = df.filter(col("bytes_sent") > 1000000)

    # Escribir las anomalías en la consola para monitoreo
    query = (
        anomalies_df.writeStream.outputMode("append")  # Modo de salida: solo filas nuevas
        .format("console")  # Mostrar resultados en la consola
        .option("truncate", "false")  # Evitar truncar las filas largas
        .start()
    )

    # Guardar los datos procesados en formato Parquet
    df.writeStream.outputMode("append").format("parquet").option(
        "path", "/opt/spark-data/network_traffic_output"  # Ruta para almacenar los datos procesados
    ).option(
        "checkpointLocation", "/opt/spark-data/network_traffic_checkpoint"  # Ruta para almacenar los checkpoints
    ).start()   

    # Mantener la aplicación en ejecución para procesar datos continuamente
    spark.streams.awaitAnyTermination()

    print("Streams closed")  # Mensaje cuando los flujos terminan


if __name__ == "__main__":
    """
    Punto de entrada del script. 
    Lee los argumentos de línea de comandos y llama a la función principal.
    """
    # Comprobar si se proporciona el argumento necesario
    if len(sys.argv) != 2:
        print("Usage: python script.py <kafka_bootstrap>")
        sys.exit(1)  # Salir con error si no se pasa el argumento

    # Leer la dirección del servidor Kafka desde los argumentos
    kafka_bootstrap = sys.argv[1]
    consume_kafka_events(kafka_bootstrap)
