from spark_session import get_spark_session
from kafka_stream import read_kafka_stream
from preprocessing import preprocess_data
from ml_model import train_kmeans_model, detect_anomalies
from alerts import generate_alerts, write_alerts_to_cassandra, write_alerts_to_kafka

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Network-Traffic-Monitor") \
    .master("local[*]") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

# Leer datos desde Kafka
traffic_logs = read_kafka_stream(spark)

# Preprocesar datos
feature_vector = preprocess_data(traffic_logs)

# Entrenar modelo K-Means
model = train_kmeans_model(feature_vector)

# Detectar anomalías
anomalies = detect_anomalies(feature_vector, model)

# Generar y escribir alertas
alerts = generate_alerts(anomalies)
cassandra_query = write_alerts_to_cassandra(alerts)
kafka_query = write_alerts_to_kafka(alerts)

# Esperar a que terminen los streams
cassandra_query.awaitTermination()
kafka_query.awaitTermination()
