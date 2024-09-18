from pyspark.sql import DataFrame

def union_cars(cars_df_1, cars_df_2, cars_df_3) -> DataFrame:
    """
    Task 1: Combine three car datasets. This method should support the case when
    there's mismatch in the schema definition or missing columns
    """
    return None

def distinct_cities(agencies_df, customers_df) -> DataFrame:
    """
    Task 2: Combine the city from the Agencies table with the city from 
    the Customers table to get all unique cities involved in car rentals.
    """
    return None

def left_join_cars_brands(cars_df, brands_df) -> DataFrame:
    """
    Task 3: Perform a left join between Cars and Brands.
    """
    return None

def right_join_cars_rentals(rentals_df, cars_df) -> DataFrame:
    """
    Task 4: Perform a right join between Cars and Rentals
    """
    return None
