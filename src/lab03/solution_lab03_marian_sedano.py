def read_csv_dataset(spark, path):
    '''
    Reads the data frame from the given path.
    '''
    df = spark.read \
        .option("mode", "FAILFAST") \
        .option("inferSchema", "true") \
        .option("header", True) \
        .csv(path)

    return df

def get_total_passengers(flights_df):
    '''
    Analyze the total number of passengers each airline has transported.
    '''
    return flights_df.groupBy("Airline").agg({"Passengers": "sum"})

def get_top_5_busiest_origin_countries(flights_df):
    '''
    Find the top 5 countries with the most outbound flights (based on the number of flights originating from each country).
    '''
    return flights_df.groupBy("OriginCountry").count().sort("count", ascending=False).limit(5)

def get_avg_passengers_per_flight_per_destination_country(flights_df):
    '''
    Calculate the average number of passengers per flight arriving in each destination country.
    '''
    return flights_df.groupBy("DestinationCountry").agg({"Passengers": "avg"})

def get_flights_specific_date(flights_df, date):
    '''
    List all flights and their details on a specific date.
    '''
    return flights_df.where(flights_df["FlightDate"] == date)

def get_total_passengers_between_two_countries(flights_df, country_a, country_b):
    '''
    Find the total number of passengers flying between two specific countries.
    '''
    return flights_df.filter((flights_df["OriginCountry"] == country_a) & 
                             (flights_df["DestinationCountry"] == country_b)) \
                     .agg({"Passengers": "sum"})

