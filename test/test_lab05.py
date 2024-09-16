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
src_dir = os.path.join(os.path.dirname(__file__), '..', 'src/lab05')


for file in os.listdir(src_dir):
    if file.lower().startswith('solution') and file.endswith('.py'):
        module_name = file[:-3]  # Remove the .py extension
        solution_modules.append(importlib.import_module(f'src.lab05.{module_name}'))


spark = SparkSession.builder.appName("UT lab05").getOrCreate()
# Load DataFrames
cars_df_a = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("datasets/cars_example/cars_a.csv")
cars_df_b = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("datasets/cars_example/cars_b.csv")
cars_df_c = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("datasets/cars_example/cars_c.csv")
brands_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("datasets/cars_example/brands.csv")
customers_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("datasets/cars_example/customers.csv")
agencies_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("datasets/cars_example/agencies.csv")

rentals_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("datasets/cars_example/rentals.csv")

# Test case for union_cars 
@pytest.mark.parametrize("solution_module", solution_modules)
def test_get_total_cars(solution_module):
    global union_cars
    union_cars = solution_module.union_cars(cars_df_a, cars_df_b, cars_df_c)
    assert union_cars.count() == 187

# Test case for distinct_cities
@pytest.mark.parametrize("solution_module", solution_modules)
def test_get_distinct_cities(solution_module):
    union_cities= solution_module.distinct_cities(agencies_df, customers_df)
    assert union_cities.count() == 112

# Test case for left_join_cars_brands
@pytest.mark.parametrize("solution_module", solution_modules)
def test_left_join_cars_brands(solution_module):
    left_join_df = solution_module.left_join_cars_brands(cars_df_a, brands_df)
    assert left_join_df.count() == 63

# Test case for right_join_customers_agencies
@pytest.mark.parametrize("solution_module", solution_modules)
def test_right_join_cars_rentals(solution_module):
    right_join_df = solution_module.right_join_cars_rentals(rentals_df, union_cars)
    assert right_join_df.count() == 13523
