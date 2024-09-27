from pyspark.sql import DataFrame

def union_cars(cars_df_1, cars_df_2, cars_df_3) -> DataFrame:
    """
    Task 1: Combine three car datasets. This method should support the case when
    there's mismatch in the schema definition or missing columns
    """
    combined_df = cars_df_1.unionByName(cars_df_2, allowMissingColumns=True) \
                           .unionByName(cars_df_3, allowMissingColumns=True)
    return combined_df

def distinct_cities(agencies_df, customers_df) -> DataFrame:
    """
    Task 2: Combine the city from the Agencies table with the city from 
    the Customers table to get all unique cities involved in car rentals.
    """
    agencies_cities = agencies_df.select("city").distinct()
    customers_cities = customers_df.select("city").distinct()
    all_cities = agencies_cities.union(customers_cities).distinct()
    return all_cities

def left_join_cars_brands(cars_df, brands_df) -> DataFrame:
    """
    Task 3: Perform a left join between Cars and Brands.
    """
    joined_df = cars_df.join(brands_df, on="brand_id", how="left")
    return joined_df

def right_join_cars_rentals(rentals_df, cars_df) -> DataFrame:
    """
    Task 4: Perform a right join between Cars and Rentals
    """
    joined_df = cars_df.join(rentals_df, on="car_id", how="right")
    return joined_df
