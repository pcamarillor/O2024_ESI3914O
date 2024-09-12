def read_csv_dataset(spark, path):
    '''
    Reads the data frame from the given path
    '''
    df = spark \
        .read \
        .format("csv") \
        .option("mode", "FAILFAST") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .option("path", "datasets/international_flights/international-flights-sql-excercise_5M.csv.gz") \
        .load()
    
    return df

def get_total_passengers(flights_df):
    '''
    Analyze the total number of passengers each airline has transported.
    '''
    total_passengers = flights_df.groupBy("Airline").sum("Passengers")
    return total_passengers

def get_top_5_busiest_origin_countries(flights_df):
    '''
    Find the top 5 countries with the most outbound flights (based on the number of flights originating from each country).
    '''
    top_5 = flights_df.groupBy("OriginCountry").count().orderBy("count",ascending=False).limit(5)
    return top_5

def get_avg_passengers_per_flight_per_destination_country(flights_df):
    '''
    Calculate the average number of passengers per flight arriving in each destination country.
    '''
    avg_passengers_per_flight = flights_df.groupBy("DestinationCountry").avg("Passengers")

    return avg_passengers_per_flight

def get_flights_specific_date(flights_df, date):
    '''
    List all flights and their details on a specific date
    '''

    flight_date_details = flights_df.filter(flights_df.FlightDate == date)

    return flight_date_details

def get_total_passengers_between_two_countries(flights_df, country_a, country_b):
    '''
    Find the total number of passengers flying between two specific countries
    '''
    total_passengers_df = flights_df.filter(
        ((flights_df.OriginCountry == country_a) & (flights_df.DestinationCountry == country_b))
    ).groupBy().sum("Passengers")
    
    return total_passengers_df
