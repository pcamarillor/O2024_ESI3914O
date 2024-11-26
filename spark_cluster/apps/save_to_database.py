from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, TimestampType, DoubleType, StructField, IntegerType

## Initialize SparkSession
spark = SparkSession.builder \
            .appName("Solution-GPS-Storage-MongoDB") \
            .config("spark.ui.port","4040") \
            .config("spark.mongodb.read.connection.uri", "mongodb://mongo-gps/project.gps") \
            .config("spark.mongodb.write.connection.uri", "mongodb://mongo-gps/project.gps") \
            .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

gps_schema = StructType([StructField("latitude", DoubleType(), True),
                                StructField("longitude", DoubleType(), True),
                                StructField("timestamp", StringType(), True),
                                StructField("speed", DoubleType(), True),
                                StructField("state", StringType(), True),
    ])

# Read the GPS Parquet Files
df = spark.read.schema(gps_schema).parquet("opt/spark-data/parquet_files/output/")

df.show()


# Write to MongoDB collection
df.write \
    .format("mongodb") \
    .option("database", "project") \
    .option("collection", "gps") \
    .mode("append") \
    .save()

print('Se ha terminado de subir los datos a la base de datos')
