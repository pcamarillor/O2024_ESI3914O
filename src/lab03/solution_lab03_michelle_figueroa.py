def read_csv_dataset(spark, path):

    flights_df = spark.read.csv(path, header=True, inferSchema=True)

    return flights_df

def get_total_passengers(flights_df):

    total_passengers_df = flights_df.groupBy("Airline").sum("Passengers")

    return total_passengers_df

def get_top_5_busiest_origin_countries(flights_df):

    top_5_countries_df = flights_df.groupBy("OriginCountry").count().orderBy("count", ascending=False).limit(5)
    
    return top_5_countries_df

def get_avg_passengers_per_flight_per_destination_country(flights_df):

    avg_passengers_df = flights_df.groupBy("DestinationCountry").avg("Passengers")

    return avg_passengers_df

def get_flights_specific_date(flights_df, date):

    specific_date_df = flights_df.filter(flights_df.FlightDate == date)

    return specific_date_df

def get_total_passengers_between_two_countries(flights_df, country_a, country_b):

    passengers_between_countries_df = flights_df.filter((flights_df["OriginCountry"] == country_a) & (flights_df["DestinationCountry"] == country_b)).groupBy("OriginCountry", "DestinationCountry").sum("Passengers")
    
    return passengers_between_countries_df