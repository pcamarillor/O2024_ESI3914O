import pytest
import os
import importlib

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *

# Import the function to be tested
# Discover all solution modules in the src directory
solution_modules = []
flights_df = None
src_dir = os.path.join(os.path.dirname(__file__), '..', 'src/lab03')


for file in os.listdir(src_dir):
    if file.lower().startswith('solution') and file.endswith('.py'):
        module_name = file[:-3]  # Remove the .py extension
        solution_modules.append(importlib.import_module(f'src.lab03.{module_name}'))

# Test case for get_total_passengers
@pytest.mark.parametrize("solution_module", solution_modules)
def test_read_df(solution_module):
    spark = SparkSession.builder.appName("UT lab03").getOrCreate()
    global flights_df
    path = "datasets/international_flights/"
    flights_df = solution_module.read_csv_dataset(spark, path)
    assert flights_df.count() == 5000000

# Test case for get_total_passengers
@pytest.mark.parametrize("solution_module", solution_modules)
def test_get_total_passengers(solution_module):
    result = solution_module.get_total_passengers(flights_df)
    print(f"Result:{result.collect()}")
    result.show()
    # There are 11 airlines
    assert result.count() == 11

# Test case for get_top_5_busiest_origin_countries
@pytest.mark.parametrize("solution_module", solution_modules)
@pytest.mark.parametrize("expected_results", [
    [Row(OriginCountry='Argentina', count=264210), Row(OriginCountry='Spain', count=263695), Row(OriginCountry='Turkey', count=263527), Row(OriginCountry='Germany', count=263523), Row(OriginCountry='Italy', count=263518)]
])
def test_get_top_5_busiest_origin_countries(solution_module, expected_results):
    result = solution_module.get_top_5_busiest_origin_countries(flights_df)
    print(f"Result:{result.collect()}")
    result.show()
    assert result.collect() == expected_results

# Test case for get_avg_passengers_per_flight_per_destination_country
@pytest.mark.parametrize("solution_module", solution_modules)
# @pytest.mark.parametrize("expected_result", [
#     [Row(DestinationCountry='Russia', avg(Passengers)=174.70079515708045), 
#      Row(DestinationCountry='Turkey', avg(Passengers)=174.81046991809325), 
#      Row(DestinationCountry='Germany', avg(Passengers)=175.2172136115025), 
#      Row(DestinationCountry='France', avg(Passengers)=174.99145674765393), 
#      Row(DestinationCountry='Greece', avg(Passengers)=175.22761625694497),
#      Row(DestinationCountry='Argentina', avg(Passengers)=175.05256951317082), 
#      Row(DestinationCountry='India', avg(Passengers)=175.11018335984656), 
#      Row(DestinationCountry='China', avg(Passengers)=175.02364707410436), 
#      Row(DestinationCountry='Italy', avg(Passengers)=174.9214558949906), 
#      Row(DestinationCountry='Spain', avg(Passengers)=174.73561883290236), 
#      Row(DestinationCountry='USA', avg(Passengers)=174.99362501422988), 
#      Row(DestinationCountry='Mexico', avg(Passengers)=175.0419949002664), 
#      Row(DestinationCountry='UK', avg(Passengers)=175.2083852536243), 
#      Row(DestinationCountry='Brazil', avg(Passengers)=175.36827871097316), 
#      Row(DestinationCountry='Japan', avg(Passengers)=175.05161810513025), 
#      Row(DestinationCountry='Poland', avg(Passengers)=174.7601732588624), 
#      Row(DestinationCountry='Egypt', avg(Passengers)=175.02533766416744), 
#      Row(DestinationCountry='Korea', avg(Passengers)=175.13648065736115), 
#      Row(DestinationCountry='Colombia', avg(Passengers)=174.91330376687773)]
# ])
def test_get_avg_passengers_per_flight_per_destination_country(solution_module):
    result = solution_module.get_avg_passengers_per_flight_per_destination_country(flights_df)
    result.show()
    print(f"Result:{result.filter(result.DestinationCountry == 'Mexico').collect()}")
    row = result.filter(result.DestinationCountry == 'Mexico').collect()[0]
    assert row["avg(Passengers)"] == 175.0419949002664

# Test case for get_flights_specific_date
@pytest.mark.parametrize("solution_module", solution_modules)
def test_get_flights_specific_date(solution_module):
    result = solution_module.get_flights_specific_date(flights_df, "2022-08-09")
    print(f"Result:{result.count()}")
    assert result.count() == 2798

# Test case for get_total_passengers_between_two_countries
@pytest.mark.parametrize("solution_module", solution_modules)
def test_get_total_passengers_between_two_countries(solution_module):
    result = solution_module.get_total_passengers_between_two_countries(flights_df, "Mexico", "Colombia")
    print(f"Result:{result.collect()}")
    result = result.collect()[0]["sum(Passengers)"]
    assert result == 2429228
