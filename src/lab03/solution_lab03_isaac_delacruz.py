def read_csv_dataset(spark, path):
    schema_str = "FlightNumber STRING, Airline STRING, OriginCountry STRING, DestinationCountry STRING, Passengers INT, FlightDate DATE"
    
    return spark \
           .read \
           .option("mode", "FAILFAST") \
           .option("header", "true") \
           .schema(schema_str) \
           .csv(path)

def get_total_passengers(flights_df):
    return flights_df.groupBy("Airline").sum("Passengers")

def get_top_5_busiest_origin_countries(flights_df):
    return flights_df.groupBy("OriginCountry").count().orderBy("count", ascending = False).limit(5)

def get_avg_passengers_per_flight_per_destination_country(flights_df):
    return flights_df.groupBy("DestinationCountry").avg("Passengers")

def get_flights_specific_date(flights_df, date):
    return flights_df.filter(flights_df["FlightDate"] == date)

def get_total_passengers_between_two_countries(flights_df, country_a, country_b):
    return flights_df.filter((flights_df["OriginCountry"] == country_a) & (flights_df["DestinationCountry"] == country_b)).groupBy().sum("Passengers")
