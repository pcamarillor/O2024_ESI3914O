from pyspark.sql.functions import count, sum, col, avg
from pyspark.sql.types import IntegerType

def read_csv_dataset(spark, path):
    '''
    Reads the data frame from the given path
    '''
    return spark.read.option("header", "true").csv(path)

def get_total_passengers(flights_df):
    '''
    Analyze the total number of passengers each airline has transported.
    '''
    return flights_df.groupBy("airline").agg(sum("passengers").alias("total_passengers"))

def get_top_5_busiest_origin_countries(flights_df):
    '''
    Find the top 5 countries with the most outbound flights (based on the number of flights originating from each country).
    '''
    return flights_df.groupBy("OriginCountry").agg(count("*").alias("total_flights")) \
                     .orderBy("total_flights", ascending=False) \
                     .limit(5)

def get_avg_passengers_per_flight_per_destination_country(flights_df):
    '''
    Calculate the average number of passengers per flight arriving in each destination country.
    '''
    flights_df = flights_df.withColumn("Passengers", col("Passengers").cast(IntegerType()))

    avg = flights_df.groupBy("DestinationCountry").avg("Passengers")
    
    return avg

def get_flights_specific_date(flights_df, date):
    '''
    List all flights and their details on a specific date
    '''

    return flights_df.filter(flights_df["FlightDate"] == date)

def get_total_passengers_between_two_countries(flights_df, country_a, country_b):
    '''
    Find the total number of passengers flying between two specific countries
    '''
    passengers_between_two_countries = flights_df.filter(
        ((col("OriginCountry") == country_a) & (col("DestinationCountry") == country_b)))\
            .agg({"Passengers": "sum"})

    return passengers_between_two_countries