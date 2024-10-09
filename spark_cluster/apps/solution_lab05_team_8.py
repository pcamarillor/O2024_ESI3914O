from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


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
    return result_df

spark = SparkSession.builder \
            .appName("lab_05") \
            .config("spark.ui.port","4040") \
            .getOrCreate()

cars_df_a = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("opt/spark-data/cars_example/cars_a.csv")
cars_df_b = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("opt/spark-data/cars_example/cars_b.csv")
cars_df_c = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("opt/spark-data/cars_example/cars_c.csv")
brands_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("opt/spark-data/cars_example/brands.csv")
customers_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("opt/spark-data/cars_example/customers.csv")
agencies_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("opt/spark-data/cars_example/agencies.csv")

rentals_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("opt/spark-data/cars_example/rentals.csv")

cars_df = union_cars(cars_df_a, cars_df_b, cars_df_c)
cars_df.show()
result = distinct_cities(agencies_df, customers_df)
result.show()
result = left_join_cars_brands(cars_df, brands_df)
result.show()
result = right_join_cars_rentals(rentals_df, cars_df)
result.show()
spark.stop()