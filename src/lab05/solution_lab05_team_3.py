from pyspark.sql import DataFrame

def union_cars(cars_df_1, cars_df_2, cars_df_3) -> DataFrame:
    """
    Task 1: Combine three car datasets. This method should support the case when
    there's mismatch in the schema definition or missing columns
    """
    resultadoParcial = cars_df_1.unionByName(cars_df_2, allowMissingColumns=True)
    result = resultadoParcial.unionByName(cars_df_3, allowMissingColumns=True)
    result.show()

    return result

def distinct_cities(agencies_df, customers_df) -> DataFrame:
    """
    Task 2: Combine the city from the Agencies table with the city from 
    the Customers table to get all unique cities involved in car rentals.
    """
    agencies_result = agencies_df.select("city")
    customers_result = customers_df.select("city")
    result = agencies_result.union(customers_result).distinct()
    return result


def left_join_cars_brands(cars_df, brands_df) -> DataFrame:
    """
    Task 3: Perform a left join between Cars and Brands.
    """
    result = cars_df.join(brands_df, on="brand_id", how="left")
    return result

def right_join_cars_rentals(rentals_df, cars_df) -> DataFrame:
    """
    Task 4: Perform a right join between Cars and Rentals
    """
    result = cars_df.join(rentals_df, on="car_id", how="right")
    return result