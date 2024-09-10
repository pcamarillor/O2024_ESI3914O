def load_csv_data(spark, file_path):
    '''
    Load a DataFrame from a CSV file located at the specified path.
    '''
    data_frame = spark.read \
        .option("mode", "FAILFAST") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .csv(file_path)

    return data_frame

def calculate_total_passengers_by_airline(flights_df):
    '''
    Compute the total passengers carried by each airline.
    '''
    return flights_df.groupBy("Airline").agg({"Passengers": "sum"})

def top_5_countries_with_most_departures(flights_df):
    '''
    Retrieve the top 5 countries with the highest number of departing flights.
    '''
    return flights_df.groupBy("OriginCountry").count().sort("count", ascending=False).limit(5)

def average_passengers_by_destination(flights_df):
    '''
    Determine the average number of passengers per flight to each destination country.
    '''
    return flights_df.groupBy("DestinationCountry").agg({"Passengers": "avg"})

def filter_flights_by_date(flights_df, specific_date):
    '''
    Get all flights on a specific date along with their details.
    '''
    return flights_df.filter(flights_df["FlightDate"] == specific_date)

def total_passengers_between_countries(flights_df, origin_country, destination_country):
    '''
    Calculate the total number of passengers traveling between two specified countries.
    '''
    return flights_df.filter((flights_df["OriginCountry"] == origin_country) & 
                             (flights_df["DestinationCountry"] == destination_country)).groupBy().agg({"Passengers": "sum"})
