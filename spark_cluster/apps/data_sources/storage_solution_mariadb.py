from pyspark.sql import SparkSession

# Inicializa SparkSession con el package de MariaDB
spark = SparkSession.builder \
            .appName("Solution-Storage-Examples-MariaDB") \
            .config("spark.ui.port", "4040") \
            .config("spark.jars.packages", "org.mariadb.jdbc:mariadb-java-client:3.1.2") \
            .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Leer el dataset de vuelos internacionales (5M)
df_flights = spark.read \
            .option("inferSchema", "true") \
            .option("header", "true") \
            .csv("/opt/spark-data/international-flights-sql-excercise_5M.csv.gz")

# Configuración del contenedor MariaDB
jdbc_url = "jdbc:mariadb://mariadb-iteso:3306/flightsdb"
df_flights.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "flights") \
    .option("user", "root") \
    .option("password", "Adm1n@1234") \
    .option("driver", "org.mariadb.jdbc.Driver") \
    .save()

print("done")