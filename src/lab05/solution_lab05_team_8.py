from pyspark.sql import DataFrame

def union_cars(cars_df_1, cars_df_2, cars_df_3) -> DataFrame:
    """
    Task 1: Combine three car datasets. This method should support the case when
    there's mismatch in the schema definition or missing columns
    """
    result_df = cars_df_1.unionByName(cars_df_2, allowMissingColumns=True)
    result_df = result_df.unionByName(cars_df_3,allowMissingColumns=True)
    return result_df


def distinct_cities(agencies_df, customers_df) -> DataFrame:
    """
    Task 2: Combine the city from the Agencies table with the city from 
    the Customers table to get all unique cities involved in car rentals.
    """
    unique_cities = agencies_df.select("city")
    unique_cities = customers_df.select("city")
    result_df = unique_cities.union(unique_cities).distinct()
    return result_df

def left_join_cars_brands(cars_df, brands_df) -> DataFrame:
    """
    Task 3: Perform a left join between Cars and Brands.
    """
    result_df = cars_df.join(brands_df,
                             cars_df['brand_id'] == brands_df['brand_id'],
                             'left')
    return result_df

def right_join_cars_rentals(rentals_df, cars_df) -> DataFrame:
    """
    Task 4: Perform a right join between Cars and Rentals
    """
    result_df = cars_df.join(rentals_df,
                             cars_df['car_id'] == rentals_df['car_id'],
                             'right')
    return result_df