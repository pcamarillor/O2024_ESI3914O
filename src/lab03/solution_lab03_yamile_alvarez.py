def read_csv_dataset(spark, path):
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
    totalp = flights_df.groupBy("Airline").sum("Passengers")
    totalp = totalp.orderBy("sum(Passengers)", ascending=False)

    return totalp

def get_top_5_busiest_origin_countries(flights_df):
    '''
    Find the top 5 countries with the most outbound flights (based on the number of flights originating from each country).
    '''
    top = flights_df.groupBy("OriginCountry").count()
    top = top.orderBy("count", ascending=False).limit(5)

    return top

def get_avg_passengers_per_flight_per_destination_country(flights_df):
    '''
    Calculate the average number of passengers per flight arriving in each destination country.
    '''
    avg = flights_df.groupBy("DestinationCountry").avg("Passengers")

    return avg


def get_flights_specific_date(flights_df, date):
    '''
    List all flights and their details on a specific date
    '''
    filterdate = flights_df.filter(flights_df["FlightDate"] ==  date)
    return filterdate


def get_total_passengers_between_two_countries(flights_df, country_a, country_b):
    '''
    Find the total number of passengers flying between two specific countries
    '''
    total = flights_df.filter(
        (flights_df["OriginCountry"] == country_a) & (flights_df["DestinationCountry"] == country_b)
    )

    suma = total.groupBy().sum("Passengers")

    return suma