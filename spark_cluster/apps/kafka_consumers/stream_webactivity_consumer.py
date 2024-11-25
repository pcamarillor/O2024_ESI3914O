
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

def consume_and_save_once(kafka_server, topics, output_path):
    # Inicializa la sesión de Spark
    spark = SparkSession.builder \
        .appName("KafkaConsumerToCSVOnce") \
        .config("spark.ui.port", "4040") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Leer datos históricos de Kafka
    kafka_df = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", topics) \
        .option("startingOffsets", "earliest") \
        .load()

    # Esquema del JSON
    schema = StructType() \
        .add("user_id", IntegerType()) \
        .add("page", StringType()) \
        .add("timestamp", StringType()) \
        .add("clicks", IntegerType())

    # Procesar JSON y extraer datos
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), schema).alias("data")) \
        .select("data.*")

    # Escribir un único archivo CSV consolidado
    parsed_df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .option("sep", ",") \
        .csv(output_path)

    print(f"Datos guardados en un único archivo CSV en {output_path}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Kafka Consumer to a Single CSV")
    parser.add_argument('--kafka-bootstrap', required=True, help="Kafka bootstrap server")
    parser.add_argument('--topics', required=True, help="Comma-separated list of Kafka topics")
    parser.add_argument('--output-path', required=True, help="Path to save the single CSV file")
    args = parser.parse_args()

    consume_and_save_once(args.kafka_bootstrap, args.topics, args.output_path)


"""

from pyspark.sql import SparkSession

def consume_kafka_events():
    # Inicializa la sesión de Spark
    spark = SparkSession.builder \
        .appName("KafkaConsumerTest") \
        .config("spark.ui.port", "4040") \
        .getOrCreate()

    # Configura el nivel de log en ERROR para reducir el ruido
    spark.sparkContext.setLogLevel("ERROR")

    # Configuración del Kafka Consumer
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9093") \
        .option("subscribe", "topicA_Mich") \
        .load()

    # Selecciona el valor del mensaje como string
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as value")

    # Escribe los mensajes en la consola
    query = parsed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    # Mantiene el stream activo
    query.awaitTermination()

if __name__ == "__main__":
    consume_kafka_events()

"""



