from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ITESO-BigData-Solution05_Team9") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

print("Spark Master URL:", sc.master)
print("Solution 5 - ITESO - Big Data - 2024")


def union_cars(cars_df_1, cars_df_2, cars_df_3) -> DataFrame:
    """
    Task 1: Combine three car datasets. This method should support the case when
    there's mismatch in the schema definition or missing columns.
    """
    valid_columns = ["car_id", "car_name", "brand_id", "price_per_day", "agency_id"]

    # Add missing columns with None (if needed)
    for col in valid_columns:
        if col not in cars_df_1.columns:
            cars_df_1 = cars_df_1.withColumn(col, lit(None).cast("string"))
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


# Sample execution of the functions to verify
if __name__ == "__main__":
    # Replace this with actual DataFrame loading from your dataset (CSV, etc.)
    # Example: cars_df_a = spark.read.option("header", "true").csv("path/to/cars_a.csv")

    # For demonstration purposes, let's assume you have DataFrames for testing:
    # Load the actual CSVs into DataFrames as appropriate
    cars_df_a = spark.createDataFrame([('1', 'Car A', 'Brand1', '100', 'Agency1')], 
                                      ['car_id', 'car_name', 'brand_id', 'price_per_day', 'agency_id'])
    cars_df_b = spark.createDataFrame([('2', 'Car B', 'Brand2', '200', 'Agency2')], 
                                      ['car_id', 'car_name', 'brand_id', 'price_per_day', 'agency_id'])
    cars_df_c = spark.createDataFrame([('3', 'Car C', 'Brand3', '300', 'Agency3')], 
                                      ['car_id', 'car_name', 'brand_id', 'price_per_day', 'agency_id'])

    # Call union_cars
    combined_cars_df = union_cars(cars_df_a, cars_df_b, cars_df_c)
    print("Union Cars Result:")
    combined_cars_df.show()

    # Assuming you have DataFrames for agencies_df and customers_df
    agencies_df = spark.createDataFrame([('City1',)], ['city'])
    customers_df = spark.createDataFrame([('City2',)], ['city'])

    # Call distinct_cities
    distinct_cities_df = distinct_cities(agencies_df, customers_df)
    print("Distinct Cities Result:")
    distinct_cities_df.show()

    # Assuming you have DataFrames for brands_df
    brands_df = spark.createDataFrame([('Brand1', 'Brand A')], ['brand_id', 'brand_name'])

    # Call left_join_cars_brands
    joined_cars_brands_df = left_join_cars_brands(cars_df_a, brands_df)
    print("Left Join Cars and Brands Result:")
    joined_cars_brands_df.show()

    # Assuming you have DataFrames for rentals_df
    rentals_df = spark.createDataFrame([('1', '2024-09-17')], ['car_id', 'rental_date'])

    # Call right_join_cars_rentals
    joined_cars_rentals_df = right_join_cars_rentals(rentals_df, combined_cars_df)
    print("Right Join Cars and Rentals Result:")
    joined_cars_rentals_df.show()

    # Stop the SparkSession when done
    spark.stop()