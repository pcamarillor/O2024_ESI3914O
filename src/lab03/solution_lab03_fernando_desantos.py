from pyspark.sql.functions import col


def read_csv_dataset(spark, path):
    '''
    Reads the data frame from the given path
    '''
    flights_df = spark.read.format("csv")\
        .option("inferSchema", "true")\
        .option("header", "true")\
        .option("path", "datasets/international_flights/international-flights-sql-excercise_5M.csv.gz")\
        .load()
    
    return flights_df

def get_total_passengers(flights_df):
    '''
    Analyze the total number of passengers each airline has transported.
    '''
    passengers_by_airline = flights_df.groupBy("Airline").sum("Passengers")

    return passengers_by_airline

def get_top_5_busiest_origin_countries(flights_df):
    '''
    Find the top 5 countries with the most outbound flights (based on the number of flights originating from each country).
    '''
    top_5_busiest_origin_countries = flights_df\
        .groupBy("OriginCountry")\
        .count()\
        .orderBy(col("count").desc())\
        .limit(5)
    
    return top_5_busiest_origin_countries

def get_avg_passengers_per_flight_per_destination_country(flights_df):
    '''
    Calculate the average number of passengers per flight arriving in each destination country.
    '''
    avg_passengers_per_flight_by_destination_country = flights_df.\
        groupBy("DestinationCountry")\
        .avg("Passengers")
    
    return avg_passengers_per_flight_by_destination_country

def get_flights_specific_date(flights_df, date):
    '''
    List all flights and their details on a specific date
    '''

    flights_on_date = flights_df.filter(col("FlightDate") == date)

    return flights_on_date

def get_total_passengers_between_two_countries(flights_df, country_a, country_b):
    '''
    Find the total number of passengers flying between two specific countries
    '''


    passengers_between_countries = flights_df.filter(
        ((col("OriginCountry") == country_a) & (col("DestinationCountry") == country_b)))\
            .agg({"Passengers": "sum"})

    return passengers_between_countries

