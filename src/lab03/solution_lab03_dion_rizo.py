from pyspark.sql import functions as F
import os

def read_csv_dataset(spark, path):
    '''
    Reads the data frame from the given path
    '''
    full_path = os.path.join(path, 'international-flights-sql-excercise_5M.csv')
    
    try:
        flights_df = spark.read.csv(path, header=True, inferSchema=True)
        flights_df = flights_df.withColumn("Passengers", F.col("Passengers").cast("int"))  
        print(f"DataFrame loaded with {flights_df.count()} records.")
        flights_df.printSchema()  
        return flights_df
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return None

def get_total_passengers(flights_df):
    '''
    Analyze the total number of passengers each airline has transported.
    '''
    total_passengers = flights_df.groupBy("Airline").agg(F.sum("Passengers"))
    return total_passengers

def get_top_5_busiest_origin_countries(flights_df):
    '''
    Find the top 5 countries with the most outbound flights (based on the number of flights originating from each country).
    '''
    busiest_countries = flights_df.groupBy("OriginCountry").count().orderBy("count", ascending=False).limit(5)
    return busiest_countries

def get_avg_passengers_per_flight_per_destination_country(flights_df):
    '''
    Calculate the average number of passengers per flight arriving in each destination country.
    '''
    passengersXflights = flights_df.groupBy("DestinationCountry").agg(F.avg("Passengers"))
    return passengersXflights

def get_flights_specific_date(flights_df, date):
    '''
    List all flights and their details on a specific date
    '''
    specific_date = flights_df.filter(flights_df.FlightDate == date)
    return specific_date

def get_total_passengers_between_two_countries(flights_df, country_a, country_b):
    '''
    Find the total number of passengers flying between two specific countries
    '''
    pB2C = flights_df.filter((flights_df["OriginCountry"] == country_a)&(flights_df["DestinationCountry"] == country_b)).groupBy().sum('Passengers')
    return pB2C

