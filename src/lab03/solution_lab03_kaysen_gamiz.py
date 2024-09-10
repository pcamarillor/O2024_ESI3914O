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
    total_passengers = flights_df.groupBy("Airline").sum("Passengers")

    return total_passengers

def get_top_5_busiest_origin_countries(flights_df):
    '''
    Find the top 5 countries with the most outbound flights (based on the number of flights originating from each country).
    '''
    busiest_countries = flights_df.groupBy("OriginCountry") \
                                  .count() \
                                  .orderBy("count", ascending=False) \
                                  .limit(5)
    
    return busiest_countries

def get_avg_passengers_per_flight_per_destination_country(flights_df):
    '''
    Calculate the average number of passengers per flight arriving in each destination country.
    '''
    flights_df = flights_df.withColumn("Passengers", flights_df["Passengers"].cast("int"))

    avg_passengers = flights_df.groupBy("DestinationCountry") \
                               .avg("Passengers").alias("avg(Passengers)")
    
    return avg_passengers

def get_flights_specific_date(flights_df, date):
    '''
    List all flights and their details on a specific date
    '''
    flights_on_date = flights_df.filter(flights_df["FlightDate"] == date)

    return flights_on_date

def get_total_passengers_between_two_countries(flights_df, country_a, country_b):
    '''
    Find the total number of passengers flying between two specific countries
    '''

    direct_flights = flights_df.filter(
        ((flights_df["OriginCountry"] == country_a) & (flights_df["DestinationCountry"] == country_b))
        )
    
    total_passengers = direct_flights.groupBy().sum("Passengers")

    return total_passengers

