def read_csv_dataset(spark, path):
    '''
    Reads the data frame from the given path
    '''

    df = spark.read.csv(path, header=True, inferSchema=True)

    return df

def get_total_passengers(flights_df):
    '''
    Analyze the total number of passengers each airline has transported.
    '''

    total_passengers_df = flights_df.groupBy("Airline").sum("Passengers")

    return total_passengers_df

def get_top_5_busiest_origin_countries(flights_df):
    '''
    Find the top 5 countries with the most outbound flights (based on the number of flights originating from each country).
    '''

    country_flight_counts_df = flights_df.groupBy("OriginCountry").count()
    
    # Sort the results by flight count in descending order
    top_5_countries_df = country_flight_counts_df.orderBy("count", ascending=False).limit(5)
    
    # Select the top 5 countries
    # top_5_countries_df = country_flight_counts_df.limit(5)

    return top_5_countries_df

def get_avg_passengers_per_flight_per_destination_country(flights_df):
    '''
    Calculate the average number of passengers per flight arriving in each destination country.
    '''

    avg_passengers_flight_destination_df = flights_df.groupBy("DestinationCountry").avg("Passengers")

    return avg_passengers_flight_destination_df

def get_flights_specific_date(flights_df, date):
    '''
    List all flights and their details on a specific date
    '''

    flights_specific_date_df = flights_df.filter(flights_df.FlightDate == date)
    
    return flights_specific_date_df

def get_total_passengers_between_two_countries(flights_df, country_a, country_b):
    '''
    Find the total number of passengers flying between two specific countries
    '''

    flights_between_countries_df = flights_df.filter(
        (flights_df.OriginCountry == country_a) & (flights_df.DestinationCountry == country_b)
    )
    
    # Sum the passengers for the filtered flights
    total_passengers = flights_between_countries_df.agg({"Passengers": "sum"})
    
    return total_passengers
