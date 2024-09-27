from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

def union_cars(cars_df_1, cars_df_2, cars_df_3) -> DataFrame:
    """
    Task 1: Combine three car datasets. This method should support the case when
    there's mismatch in the schema definition or missing columns.
    """
    # Manually select known valid columns
    valid_columns = ["car_id", "car_name", "brand_id", "price_per_day", "agency_id"]

    # Add missing columns with None (if needed)
    for col in valid_columns:
        if col not in cars_df_1.columns:
            cars_df_1 = cars_df_1.withColumn(col, lit(None).cast("string"))  # Assuming string type for simplicity
        if col not in cars_df_2.columns:
            cars_df_2 = cars_df_2.withColumn(col, lit(None).cast("string"))
        if col not in cars_df_3.columns:
            cars_df_3 = cars_df_3.withColumn(col, lit(None).cast("string"))

    # Drop columns with problematic datatypes (like 'void')
    try:
        cars_df_1 = cars_df_1.select(valid_columns)
        cars_df_2 = cars_df_2.select(valid_columns)
        cars_df_3 = cars_df_3.select(valid_columns)
    except Exception as e:
        print(f"Error selecting valid columns: {e}")
        raise ValueError("Problematic datatype detected, check data source.")

    # Perform the union operation
    combined_cars = cars_df_1.unionByName(cars_df_2).unionByName(cars_df_3)
    
    return combined_cars

def distinct_cities(agencies_df, customers_df) -> DataFrame:
    """
    Task 2: Combine the city from the Agencies table with the city from 
    the Customers table to get all unique cities involved in car rentals.
    """
    agencies_cities = agencies_df.select('city')
    customers_cities = customers_df.select('city')
    
    all_cities = agencies_cities.union(customers_cities).distinct()
    return all_cities

def left_join_cars_brands(cars_df, brands_df) -> DataFrame:
    """
    Task 3: Perform a left join between Cars and Brands.
    """
    if 'id' in brands_df.columns and 'brand_id' not in brands_df.columns:
        brands_df = brands_df.withColumnRenamed('id', 'brand_id')
    
    joined_df = cars_df.join(brands_df, on='brand_id', how='left')
    return joined_df

def right_join_cars_rentals(rentals_df, cars_df) -> DataFrame:
    """
    Task 4: Perform a right join between Cars and Rentals.
    """
    joined_df = cars_df.join(rentals_df, on='car_id', how='right')
    return joined_df
