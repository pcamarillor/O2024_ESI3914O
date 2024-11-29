from pyspark.sql.functions import count, mean
from pyspark.ml.feature import VectorAssembler

def preprocess_data(traffic_logs):
    # Agrupar por IP de origen para generar características
    feature_data = traffic_logs \
        .groupBy("source_ip") \
        .agg(
            count("*").alias("packet_count"),
            mean("packet_size").alias("avg_packet_size")
        )
    
    # Vectorizar las características
    assembler = VectorAssembler(
        inputCols=["packet_count", "avg_packet_size"],
        outputCol="features"
    )
    feature_vector = assembler.transform(feature_data)

    return feature_vector
