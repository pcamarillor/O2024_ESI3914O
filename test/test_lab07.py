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
src_dir = os.path.join(os.path.dirname(__file__), '..', 'src/lab07')


for file in os.listdir(src_dir):
    if file.lower().startswith('solution') and file.endswith('.py'):
        module_name = file[:-3]  # Remove the .py extension
        solution_modules.append(importlib.import_module(f'src.lab07.{module_name}'))


spark = SparkSession.builder \
        .appName("Lab 07 - Persisting Netflix data") \
        .config("spark.driver.bindAddress", "localhost") \
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Load DataFrames
netflix_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("datasets/netflix1.csv")

@pytest.mark.parametrize("solution_module", solution_modules)
def test_clean_df(solution_module):
    data = [(0, 0, 0, 0, 0, 0, 0, 0, 0, 0)]
    df_result = solution_module.clean_df(netflix_df)
    df_nulls = df_result.select([count(when(col(c).isNull(), c)).alias(c) for c in df_result.columns])
    assert df_nulls.collect() == data

