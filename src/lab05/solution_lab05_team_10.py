from pyspark.sql import SparkSession, DataFrame


def union_cars(cars_df_1, cars_df_2, cars_df_3) -> DataFrame:
    """
    Task 1: Combine three car datasets. This method should support the case when
    there's mismatch in the schema definition or missing columns.
    """
    combined_df = (
        cars_df_1.unionByName(cars_df_2, allowMissingColumns=True)
                .unionByName(cars_df_3, allowMissingColumns=True)
                .distinct()  
    )
    return combined_df


def distinct_cities(agencies_df, customers_df) -> DataFrame:
    """
    Task 2: Combine the city from the Agencies table with the city from 
    the Customers table to get all unique cities involved in car rentals.
    """
    cities_df = agencies_df.select("city").union(customers_df.select("city")).distinct()
    return cities_df


def left_join_cars_brands(cars_df, brands_df) -> DataFrame:
    """
    Task 3: Perform a left join between Cars and Brands.
    """
    left_join_df = cars_df.join(brands_df, on="brand_id", how="left")
    return left_join_df


def right_join_cars_rentals(rentals_df, cars_df) -> DataFrame:
    """
    Task 4: Perform a right join between Cars and Rentals.
    """
    right_join_df = cars_df.join(rentals_df, on="car_id", how="right")
    return right_join_df



def main():
    
    spark = SparkSession.builder.appName("Lab05-Solution").getOrCreate()


    cars_df_a = spark.read.csv("/opt/spark-data/cars_a.csv", header=True, inferSchema=True)
    cars_df_b = spark.read.csv("/opt/spark-data/cars_b.csv", header=True, inferSchema=True)
    cars_df_c = spark.read.csv("/opt/spark-data/cars_c.csv", header=True, inferSchema=True)
    
    rentals_df = spark.read.csv("/opt/spark-data/rentals.csv", header=True, inferSchema=True)
    agencies_df = spark.read.csv("/opt/spark-data/agencies.csv", header=True, inferSchema=True)
    customers_df = spark.read.csv("/opt/spark-data/customers.csv", header=True, inferSchema=True)
    brands_df = spark.read.csv("/opt/spark-data/brands.csv", header=True, inferSchema=True)


    print("Task 1: Union of cars datasets")
    combined_cars_df = union_cars(cars_df_a, cars_df_b, cars_df_c)
    combined_cars_df.show()

    print("Task 2: Distinct cities involved in car rentals")
    cities_df = distinct_cities(agencies_df, customers_df)
    cities_df.show()

    print("Task 3: Left join between cars and brands")
    left_join_df = left_join_cars_brands(combined_cars_df, brands_df)
    left_join_df.show()

    print("Task 4: Right join between cars and rentals")
    right_join_df = right_join_cars_rentals(rentals_df, combined_cars_df)
    right_join_df.show()

    
    spark.stop()


if __name__ == "__main__":
    main()
