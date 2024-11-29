import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count

def process_network_traffic_and_save_to_cassandra(cassandra_config):
    spark = (
        SparkSession.builder.appName("Network-Traffic-Analysis-Results")
        .config("spark.ui.port", "4040")
        .config("spark.cassandra.connection.host", cassandra_config["host"])
        .config("spark.cassandra.connection.port", cassandra_config["port"])
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    network_traffic_df = spark.read.parquet("/opt/spark-data/network_traffic_data/")

    peak_activity_df = (
        network_traffic_df.groupBy("source_ip", "destination_ip", "event_time")
        .agg(count("*").alias("request_count"))
        .filter(col("request_count") > 1000)
    )

    data_transfer_anomalies_df = (
        network_traffic_df.groupBy("source_ip", "destination_ip")
        .agg(sum("data_transferred").alias("total_data_transferred"))
        .filter(col("total_data_transferred") > 500000)
    )

    peak_activity_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="peak_activity", keyspace=cassandra_config["keyspace"]) \
        .mode("overwrite") \
        .save()

    data_transfer_anomalies_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="data_transfer_anomalies", keyspace=cassandra_config["keyspace"]) \
        .mode("overwrite") \
        .save()

    print("Results uploaded to Cassandra: Peak activity and data transfer anomalies.")

    spark.stop()

if __name__ == "__main__":
    cassandra_config = {
        "host": "cassandra-node",
        "port": "9042",
        "keyspace": "network_traffic",
    }

    process_network_traffic_and_save_to_cassandra(cassandra_config)
