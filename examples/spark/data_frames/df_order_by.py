from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql import Row
from datetime import datetime

# Initialize SparkSession
spark = SparkSession.builder.appName("Order by example").getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("FlightNumber", StringType(), True),
    StructField("Airline", StringType(), True),
    StructField("OriginCountry", StringType(), True),
    StructField("DestinationCountry", StringType(), True),
    StructField("Passengers", IntegerType(), True),
    StructField("FlightDate", DateType(), True)
])

# Create a list of Rows with flight data
data = [
    Row("AA123", "American Airlines", "USA", "Canada", 180, datetime.strptime("2024-09-01", "%Y-%m-%d")),
    Row("DL456", "Delta Airlines", "USA", "Mexico", 150, datetime.strptime("2024-09-02", "%Y-%m-%d")),
    Row("UA789", "United Airlines", "Canada", "USA", 220, datetime.strptime("2024-09-03", "%Y-%m-%d")),
    Row("SW001", "Southwest", "USA", "Canada", 160, datetime.strptime("2024-09-01", "%Y-%m-%d")),
    Row("AV123", "Avianca", "Colombia", "USA", 200, datetime.strptime("2024-09-04", "%Y-%m-%d"))
]

# Create the DataFrame using the schema and the data
flights_df = spark.createDataFrame(data, schema)

# Show the DataFrame
flights_df.show()

flights_df.orderBy("Passengers", ascending=False).show()

flights_df.filter(flights_df["Passengers"] >= 180).show()

flights_df.filter((flights_df["Passengers"] >= 180) & (flights_df["DestinationCountry"] != "USA")).show()

flights_df.filter(flights_df["Passengers"] > 150) \
  .orderBy("FlightDate", ascending=True) \
  .show()

spark.sparkContext.stop()