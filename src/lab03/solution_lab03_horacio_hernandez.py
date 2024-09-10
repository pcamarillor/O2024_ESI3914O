def read_csv_dataset(spark, path):
    df = spark \
        .read \
        .format("csv") \
        .option("mode", "FAILFAST") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .option("path", "/home/horacio/school/8thSemester/bigData/CodeLab00/O2024_ESI3914O/datasets/international-flights-sql-excercise_5M.csv") \
        .load()

    if(path == "/home/horacio/school/8thSemester/bigData/CodeLab00/O2024_ESI3914O/datasets/international-flights-sql-excercise_5M.csv"):
        print(f'File found...')

    return df


def get_total_passengers(flights_df):
    total_passengers = flights_df.groupBy("Airline").sum("Passengers")
    total_passengers.show()
    return total_passengers

def get_top_5_busiest_origin_countries(flights_df):
    '''
    Find the top 5 countries with the most outbound flights (based on the number of flights originating from each country).
    '''
    countries = flights_df.groupBy("OriginCountry").count()
    top_5_countries = countries.orderBy("count", ascending=False).limit(5)
    return top_5_countries

def get_avg_passengers_per_flight_per_destination_country(flights_df):
    '''
    Calculate the average number of passengers per flight arriving in each destination country.
    '''

    #passengers = flights_df.groupBy("DestinationCountry")
    #.agg(sum(flights_df["Passengers"]))
    #average = passengers / flights_df.count(flights_df["FlightNumber"])
    return flights_df.groupBy("DestinationCountry").avg("Passengers").alias('avg(Passengers)')


def get_flights_specific_date(flights_df, date):
    '''
    List all flights and their details on a specific date
    '''

    flights = flights_df.where(flights_df["FlightDate"] == date)
    return flights

def get_total_passengers_between_two_countries(flights_df, country_a, country_b):
    '''
    Find the total number of passengers flying between two specific countries
    '''

    flights = flights_df.where((flights_df["OriginCountry"] == country_a) & (flights_df["DestinationCountry"] == country_b))
    total_passengers_btw = flights.groupBy().sum("Passengers")

    return total_passengers_btw
