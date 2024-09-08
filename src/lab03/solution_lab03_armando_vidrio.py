from pyspark.sql.functions import count
def read_csv_dataset(spark, path):
    '''Create a dataframe'''
    df = spark.read \
        .format("csv") \
        .option("mode", "FAILFAST") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .option("path", path) \
        .load()
    return df

def get_total_passengers(flights_df):
    '''
    Analyze the total number of passengers each airline has transported.
    '''
    total_pass = flights_df.groupBy("Airline").sum('Passengers')
    return total_pass

def get_top_5_busiest_origin_countries(flights_df):
    '''
    Find the top 5 countries with the most outbound flights (based on the number of flights originating from each country).
    '''
    outbound_flights_country = flights_df.groupBy("OriginCountry") \
        .agg(count("FlightNumber")) \
        .orderBy('count(FlightNumber)', ascending=False) \
        .limit(5)

    return outbound_flights_country

def get_avg_passengers_per_flight_per_destination_country(flights_df):
    '''
    Calculate the average number of passengers per flight arriving in each destination country.
    '''
    destinations = flights_df.groupBy("DestinationCountry") \
        .avg("Passengers")
    
    return destinations

def get_flights_specific_date(flights_df, date):
    '''
    List all flights and their details on a specific date
    '''
    flights_at_x_date = flights_df.filter(flights_df['FlightDate'] == date)
    
    return flights_at_x_date

def get_total_passengers_between_two_countries(flights_df, country_a, country_b):
    '''
    Find the total number of passengers flying between two specific countries
    '''
    passengers_between_2_countries = flights_df.filter((flights_df["OriginCountry"] == country_a) & (flights_df["DestinationCountry"] == country_b))

    temp_df = passengers_between_2_countries.groupBy("OriginCountry") \
        .sum("Passengers")

    total = temp_df.select("sum(Passengers)")

    return total