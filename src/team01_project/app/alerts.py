from pyspark.sql.functions import lit

def generate_alerts(anomalies):
    alerts = anomalies.select(
        "source_ip",
        "packet_count",
        "avg_packet_size",
        lit("Anomalous Behavior Detected").alias("alert_description")
    )
    return alerts

def write_alerts_to_cassandra(alerts, keyspace="network_traffic_data", table="alerts"):
    query = alerts.writeStream \
        .outputMode("append") \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", keyspace) \
        .option("table", table) \
        .start()
    return query

def write_alerts_to_kafka(alerts, kafka_broker="localhost:9092", topic="traffic_alerts"):
    kafka_query = alerts.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .outputMode("append") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("topic", topic) \
        .start()
    return kafka_query
