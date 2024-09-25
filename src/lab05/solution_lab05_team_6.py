from pyspark.sql import DataFrame
#from pyspark.sql import SparkSession

def union_cars(cars_df_1, cars_df_2, cars_df_3) -> DataFrame:
    """
    Task 1: Combine three car datasets. This method should support the case when
    there's mismatch in the schema definition or missing columns
    """
    df_result = cars_df_1.unionByName(cars_df_2, allowMissingColumns=True) \
                .unionByName(cars_df_3 , allowMissingColumns=True)
    df_result.show()
    return df_result

def distinct_cities(agencies_df, customers_df) -> DataFrame:
    """
    Task 2: Combine the city from the Agencies table with the city from 
    the Customers table to get all unique cities involved in car rentals.
    """
    agencies_cities_df = agencies_df.select('city')
    customers_cities_df = customers_df.select('city')
    
    df_result = agencies_cities_df.union(customers_cities_df).distinct()
    df_result.show()
    return df_result


def left_join_cars_brands(cars_df, brands_df) -> DataFrame:
    result_df = cars_df.join(brands_df, cars_df['brand_id'] == brands_df['brand_id'], "left")
    result_df.show()
    return result_df

def right_join_cars_rentals(rentals_df, cars_df) -> DataFrame:
    """
    Task 4: Perform a right join between Cars and Rentals
    """
    result_df = cars_df.join(rentals_df, on='car_id', how="right").distinct()
    result_df.show()
    return result_df

"""

spark = SparkSession.builder.appName("UT lab05").getOrCreate()

cars_df_a = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("/opt/spark-data/cars_a.csv")
cars_df_b = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("/opt/spark-data/cars_b.csv")
cars_df_c = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("/opt/spark-data/cars_c.csv")
brands_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("/opt/spark-data/brands.csv")
customers_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("/opt/spark-data/customers.csv")
agencies_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("/opt/spark-data/agencies.csv")

rentals_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("/opt/spark-data/rentals.csv")

union_cars(cars_df_a, cars_df_b, cars_df_c)
distinct_cities(agencies_df, customers_df)
left_join_cars_brands(cars_df_a, brands_df)
right_join_cars_rentals(rentals_df, cars_df_a)
"""

