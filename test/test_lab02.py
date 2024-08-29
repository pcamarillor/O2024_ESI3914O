import pytest
import os
import importlib

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.rdd import RDD

# Import the function to be tested
# Discover all solution modules in the src directory
solution_modules = []
src_dir = os.path.join(os.path.dirname(__file__), '..', 'src/lab02')

for file in os.listdir(src_dir):
    if file.lower().startswith('solution') and file.endswith('.py'):
        module_name = file[:-3]  # Remove the .py extension
        solution_modules.append(importlib.import_module(f'src.lab02.{module_name}'))

@pytest.mark.parametrize("solution_module", solution_modules)
def test_analyze_log(solution_module):

    sc = SparkContext("local", "Test")

    # Load the log file into an RDD
    log_file_path = "datasets/access.log"
    log_rdd = sc.textFile(log_file_path)

    # Call the function to analyze the log
    result = solution_module.analyze_log(log_rdd)

    sc.stop()

    # Define expected results
    expected_result = {
        '54.36.149.41': 1,
        '31.56.96.51': 4,
        '40.77.167.129': 10,
        '91.99.72.15': 4,
        '66.249.66.194': 5,
        '207.46.13.136': 3,
        '178.253.33.51': 6,
        '66.249.66.91': 2,
        '5.78.198.52': 3,
        '34.247.132.53': 1,
        '54.36.149.70': 1,
        '2.177.12.140': 12, 
    }

    # Assert that the result matches the expected result
    assert result == expected_result, f"Expected {expected_result} but got {result}"
