from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Read CSV").getOrCreate()

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
    
    df = flights_df.groupBy('Airline') \
                    .sum('Passengers')
    
    return df

def get_top_5_busiest_origin_countries(flights_df):
    
    df = flights_df.groupBy('OriginCountry') \
                    .count() \
                    .orderBy("count", ascending = False) \
                    .limit(5)
    return df
    

def get_avg_passengers_per_flight_per_destination_country(flights_df):
    df = flights_df.groupBy('DestinationCountry') \
                    .avg('Passengers')

    return df

def get_flights_specific_date(flights_df, date):
    df = flights_df.filter(flights_df['FlightDate'] == date)

    return df

def get_total_passengers_between_two_countries(flights_df, country_a, country_b):
    df = flights_df.filter((flights_df['OriginCountry'] == country_a) & (flights_df['DestinationCountry'] == country_b)) 
    
    total_passengers = df.groupBy() \
                        .sum('Passengers')
    
    return total_passengers

