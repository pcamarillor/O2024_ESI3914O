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
src_dir = os.path.join(os.path.dirname(__file__), '..', 'src/lab06')


for file in os.listdir(src_dir):
    if file.lower().startswith('solution') and file.endswith('.py'):
        module_name = file[:-3]  # Remove the .py extension
        solution_modules.append(importlib.import_module(f'src.lab06.{module_name}'))


spark = SparkSession.builder.appName("Lab 06 - RentalCarAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

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

cars_df = cars_df_a.unionByName(cars_df_b, allowMissingColumns=True).unionByName(cars_df_c, allowMissingColumns=True)

@pytest.mark.parametrize("solution_module", solution_modules)
def test_register_data_frames(solution_module):
    solution_module.register_data_frames(cars_df, brands_df, customers_df, agencies_df, rentals_df)
    assert spark.sql("SELECT * FROM cars").count() == 187
    assert spark.sql("SELECT * FROM brands").count() == 16
    assert spark.sql("SELECT * FROM customers").count() == 17329
    assert spark.sql("SELECT * FROM agencies").count() == 5
    assert spark.sql("SELECT * FROM rentals").count() == 13523

@pytest.mark.parametrize("solution_module", solution_modules)
def test_get_three_most_popular_car_brands(solution_module):
    result = [Row(brand_name='Chevrolet', rental_count=3756),
              Row(brand_name='Nissan', rental_count=1402),
              Row(brand_name='BMW', rental_count=1278)]
    df_restult = spark.createDataFrame(result)
    assert solution_module.get_three_most_popular_car_brands(spark).collect() == df_restult.collect()

@pytest.mark.parametrize("solution_module", solution_modules)
def test_calculate_total_revenue(solution_module):
    result = [Row(agency_name='NYC Rentals',total_revenue=7291747),
              Row(agency_name='Zapopan Auto',total_revenue=2368180),
              Row(agency_name='Mexico Cars',total_revenue=2087040),
              Row(agency_name='LA Car Rental',total_revenue=-1746880),
              Row(agency_name='SF Cars',total_revenue=-2476795)]
    result_df = spark.createDataFrame(result)
    assert solution_module.calculate_total_revenue(spark).collect() == result_df.collect()

@pytest.mark.parametrize("solution_module", solution_modules)
def test_find_top_5_customers(solution_module):
    result = [Row(customer_name='Samantha Livingston', total_spent=321048),
              Row(customer_name='Christine Jones', total_spent=294335),
              Row(customer_name='Anthony King', total_spent=287185),
              Row(customer_name='Justin Ramirez', total_spent=281292),
              Row(customer_name='Alexander Hendrix', total_spent=270896)]
    assert solution_module.find_top_5_customers(spark).collect() == result

@pytest.mark.parametrize("solution_module", solution_modules)
def test_get_avg_age_customers_by_city(solution_module):
    result = [
    Row(city="North Jenniferfurt", average_age=42.11340206185567),
    Row(city="Meganbury", average_age=41.51587301587302),
    Row(city="Port Karafort", average_age=41.40909090909091),
    Row(city="Michaelside", average_age=41.333333333333336),
    Row(city="Port Jessicatown", average_age=41.00729927007299),
    Row(city="New Laurenside", average_age=40.9140625),
    Row(city="New Ronaldville", average_age=40.84615384615385),
    Row(city="Seanfurt", average_age=40.73873873873874),
    Row(city="Wadetown", average_age=40.656565656565654),
    Row(city="Bartonton", average_age=40.608695652173914)
]
    result_df = spark.createDataFrame(result)
    assert solution_module.get_avg_age_customers_by_city(spark).collect() == result_df.collect()

@pytest.mark.parametrize("solution_module", solution_modules)
def test_find_car_models_most_revenue(solution_module):
    result = [
    Row(car_name="Sienna", revenue=1912966),
    Row(car_name="Clubman", revenue=1362546),
    Row(car_name="Sundance", revenue=1149720),
    Row(car_name="Crown Victoria", revenue=1134090),
    Row(car_name="Prius Prime", revenue=1107792)
]
    result_df = spark.createDataFrame(result)
    assert solution_module.find_car_models_most_revenue(spark).collect() == result_df.collect()
