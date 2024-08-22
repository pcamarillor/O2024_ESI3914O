# test/test_lab00.py
import pytest
import os
import importlib

# Discover all solution modules in the src directory
solution_modules = []
src_dir = os.path.join(os.path.dirname(__file__), '..', 'src')

for file in os.listdir(src_dir):
    if file.lower().startswith('solution') and file.endswith('.py'):
        module_name = file[:-3]  # Remove the .py extension
        solution_modules.append(importlib.import_module(f'src.{module_name}'))



# Parametrize the test function to run for each solution module
@pytest.mark.parametrize("solution_module", solution_modules)
def test_missing_number(solution_module):

    # Define the test cases
    test_cases = [
        ([3, 0, 1], 2),
        ([0, 1], 2),
        ([9,6,4,2,3,5,7,0,1], 8),
        ([0], 1)
    ]

    # Run each test case
    for nums, expected in test_cases:
        assert solution_module.solution(nums) == expected
