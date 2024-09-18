from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

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

# Crear SparkSession
spark = SparkSession.builder.appName("Cars Analysis").getOrCreate()

# Cargar los CSVs en DataFrames de Spark
cars_a_spark = spark.read.csv('/opt/spark-data/cars_example/cars_a.csv', header=True, inferSchema=True)
cars_b_spark = spark.read.csv('/opt/spark-data/cars_example/cars_b.csv', header=True, inferSchema=True)
cars_c_spark = spark.read.csv('/opt/spark-data/cars_example/cars_c.csv', header=True, inferSchema=True)
brands_spark = spark.read.csv('/opt/spark-data/cars_example/brands.csv', header=True, inferSchema=True)
rentals_spark = spark.read.csv('/opt/spark-data/cars_example/rentals.csv', header=True, inferSchema=True)
agencies_spark = spark.read.csv('/opt/spark-data/cars_example/agencies.csv', header=True, inferSchema=True)
customers_spark = spark.read.csv('/opt/spark-data/cars_example/customers.csv', header=True, inferSchema=True)

# Ejemplo de uso
cars_combined_spark = union_cars(cars_a_spark, cars_b_spark, cars_c_spark)
unique_cities_spark = distinct_cities(agencies_spark, customers_spark)
cars_brands_left_join_spark = left_join_cars_brands(cars_combined_spark, brands_spark)
cars_rentals_right_join_spark = right_join_cars_rentals(cars_combined_spark, rentals_spark)

# Mostrar los resultados
cars_combined_spark.show(5)
unique_cities_spark.show(5)
cars_brands_left_join_spark.show(5)
cars_rentals_right_join_spark.show(5)

# Cerrar SparkSession
spark.stop()