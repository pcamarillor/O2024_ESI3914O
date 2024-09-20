from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

def register_data_frames(cars_df, brands_df, customers_df, agencies_df, rentals_df):
    return None

def get_three_most_popular_car_brands(spark) -> DataFrame:
    return None

def calculate_total_revenue(spark) -> DataFrame:
    return None 

def find_top_5_customers(spark) -> DataFrame:
    return None 

def get_avg_age_customers_by_city(spark) -> DataFrame:
    return None 

def find_car_models_most_revenue(spark) -> DataFrame:
    return None 

if __name__ == "__main__":
    # Initialize SparkSession
    spark = SparkSession.builder.appName("Lab 06 - RentalCarAnalysis").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Load DataFrames
    cars_df_a = spark.read\
                .option("header", "true")\
                .option("inferSchema", "true")\
                .csv("/opt/spark-data/rentals/cars_a.csv")
    cars_df_b = spark.read\
                .option("header", "true")\
                .option("inferSchema", "true")\
                .csv("/opt/spark-data/rentals/cars_b.csv")
    cars_df_c = spark.read\
                .option("header", "true")\
                .option("inferSchema", "true")\
                .csv("/opt/spark-data/rentals/cars_c.csv")
    brands_df = spark.read\
                .option("header", "true")\
                .option("inferSchema", "true")\
                .csv("/opt/spark-data/rentals/brands.csv")
    customers_df = spark.read\
                .option("header", "true")\
                .option("inferSchema", "true")\
                .csv("/opt/spark-data/rentals/customers.csv")
    agencies_df = spark.read\
                .option("header", "true")\
                .option("inferSchema", "true")\
                .csv("/opt/spark-data/rentals/agencies.csv")
    
    rentals_df = spark.read\
                .option("header", "true")\
                .option("inferSchema", "true")\
                .csv("/opt/spark-data/rentals/rentals.csv")

    cars = cars_df_a.unionByName(cars_df_b, allowMissingColumns=True).unionByName(cars_df_c, allowMissingColumns=True)

    register_data_frames(cars, brands_df, customers_df, agencies_df, rentals_df)
    get_three_most_popular_car_brands(spark).show(n=5, truncate=False)
    calculate_total_revenue(spark).show(n=5, truncate=False)
    find_top_5_customers(spark).show(n=5, truncate=False)
    get_avg_age_customers_by_city(spark).show(n=5, truncate=False)
    find_car_models_most_revenue(spark).show(n=5, truncate=False)
