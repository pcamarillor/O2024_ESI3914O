from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
            .appName("Solution-Storage-Examples-Postgres") \
            .config("spark.ui.port","4040") \
            .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read the international flights dataset (5M)
df_flights = spark.read \
            .option("inferSchema", "true") \
            .option("header", "true") \
            .csv("/opt/spark-data/international-flights-sql-excercise_5M.csv.gz")

# PostgreSQL container configuration
jdbc_url = "jdbc:postgresql://postgres-iteso:5432/postgres"
df_flights.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "flights") \
    .option("user", "postgres") \
    .option("password", "Adm1n@1234") \
    .option("driver", "org.postgresql.Driver") \
    .save()

print("done")






