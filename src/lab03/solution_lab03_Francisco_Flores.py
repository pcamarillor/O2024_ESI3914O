def read_csv_dataset(spark, path):
    flights_df = spark.read\
        .format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .option("mode", "FAILFAST")\
        .load(path)
    return flights_df

def get_total_passengers(flights_df):
    return flights_df.groupBy("Airline").sum("Passengers")

def get_top_5_busiest_origin_countries(flights_df):
    flights_grouped = flights_df.groupBy("OriginCountry").count()
    flights_ordered = flights_grouped.orderBy("count", ascending=False)
    return flights_ordered.limit(5)

def get_avg_passengers_per_flight_per_destination_country(flights_df):
    flights_grouped = flights_df.groupBy("DestinationCountry").avg("Passengers")
    return flights_grouped

def get_flights_specific_date(flights_df, date):
    return flights_df.filter(flights_df.FlightDate == date)

def get_total_passengers_between_two_countries(flights_df, country_a, country_b):
    flights_filtered = flights_df.filter(
        (flights_df.OriginCountry == country_a) & (flights_df.DestinationCountry == country_b)
    )
    return flights_filtered.groupBy().sum("Passengers")

'''
6 passed
'''