from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
            .appName("Solution-Storage-Examples") \
            .config("spark.ui.port","4040") \
            .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read the international flights dataset (5M)

df_flights = spark.read \
            .option("inferSchema", "true") \
            .option("header", "true") \
            .csv("/opt/spark-data/international-flights-sql-excercise_5M.csv.gz")

df_flights.printSchema()



