from pyspark.sql import SparkSession, DataFrame

def union_cars(cars_df_1, cars_df_2, cars_df_3) -> DataFrame:
    df = cars_df_1.unionByName(cars_df_2, allowMissingColumns = True).unionByName(cars_df_3, allowMissingColumns = True)
    df.show(n=10)
    return df

def distinct_cities(agencies_df, customers_df) -> DataFrame:
    df = agencies_df.select("city").union(customers_df.select("city")).distinct()
    df.show(n=10)
    return df

def left_join_cars_brands(cars_df, brands_df) -> DataFrame:
    df = cars_df.join(brands_df, on = "brand_id", how = "left")
    df.show(n=10)
    return df

def right_join_cars_rentals(rentals_df, cars_df) -> DataFrame:
    df = cars_df.join(rentals_df, on = "car_id", how = "right")
    df.show(n=10)
    return df

spark = SparkSession.builder \
    .appName("ITESO-BigData-Hello-World-App") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

cars_df_a = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("/opt/spark-data/cars_example/cars_a.csv")
cars_df_b = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("/opt/spark-data/cars_example/cars_b.csv")
cars_df_c = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("/opt/spark-data/cars_example/cars_c.csv")
brands_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("/opt/spark-data/cars_example/brands.csv")
customers_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("/opt/spark-data/cars_example/customers.csv")
agencies_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("/opt/spark-data/cars_example/agencies.csv")

rentals_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("/opt/spark-data/cars_example/rentals.csv")


union_cars = union_cars(cars_df_a, cars_df_b, cars_df_c)
distinct_cities = distinct_cities(agencies_df, customers_df)
left_join_cars_brands = left_join_cars_brands(cars_df_a, brands_df)
right_join_cars_rentals = right_join_cars_rentals(rentals_df, union_cars)

spark.stop()
