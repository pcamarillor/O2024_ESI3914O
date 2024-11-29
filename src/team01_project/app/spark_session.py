from pyspark.sql import SparkSession

def get_spark_session():
    return SparkSession.builder.master("local").\
    appName("Network-Traffic-Monitor").\
    getOrCreate()
