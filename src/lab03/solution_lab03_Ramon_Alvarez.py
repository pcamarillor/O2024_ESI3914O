# Función para leer un dataset CSV con Spark
def read_csv_dataset(spark, path):
    flights_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("mode", "FAILFAST") \
        .load(path)
    return flights_df

# Función para obtener el total de pasajeros agrupados por aerolínea
def get_total_passengers(flights_df):
    return flights_df.groupBy("Airline").sum("Passengers")

# Función para obtener los 5 países de origen más ocupados
def get_top_5_busiest_origin_countries(flights_df):
    flights_grouped = flights_df.groupBy("OriginCountry").count()
    flights_ordered = flights_grouped.orderBy("count", ascending=False)
    return flights_ordered.limit(5)

# Función para obtener el promedio de pasajeros por vuelo en cada país de destino
def get_avg_passengers_per_flight_per_destination_country(flights_df):
    flights_grouped = flights_df.groupBy("DestinationCountry").avg("Passengers")
    return flights_grouped

# Función para obtener los vuelos de una fecha específica
def get_flights_specific_date(flights_df, date):
    return flights_df.filter(flights_df.FlightDate == date)

# Función para obtener el total de pasajeros entre dos países
def get_total_passengers_between_two_countries(flights_df, country_a, country_b):
    flights_filtered = flights_df.filter(
        (flights_df.OriginCountry == country_a) & (flights_df.DestinationCountry == country_b)
    )
    return flights_filtered.groupBy().sum("Passengers")
