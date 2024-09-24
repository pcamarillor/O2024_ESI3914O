from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

def register_data_frames(cars_df, brands_df, customers_df, agencies_df, rentals_df):
    cars_df.createOrReplaceTempView("cars")
    brands_df.createOrReplaceTempView("brands")
    customers_df.createOrReplaceTempView("customers")
    agencies_df.createOrReplaceTempView("agencies")
    rentals_df.createOrReplaceTempView("rentals")

    print(f'---Schema for cars---\n')
    cars_df.printSchema()
    print(f'---Schema for brands---\n')
    brands_df.printSchema()
    print(f'---Schema for customers---\n')
    customers_df.printSchema()
    print(f'---Schema for agencies---\n')
    agencies_df.printSchema()
    print(f'---Schema for rentals---\n')
    rentals_df.printSchema()

def get_three_most_popular_car_brands(spark) -> DataFrame:
    query = """
        Select b.brand_name, COUNT(r.rental_id) as rental_counts
        From cars c
        Join rentals r on c.car_id = r.car_id
        Join brands b on c.brand_id = b.brand_id
        Group by b.brand_name
        Order by rental_counts DESC
        LIMIT 3
    """
    mpcb = spark.sql(query)
    return mpcb

def calculate_total_revenue(spark) -> DataFrame:
    query = """
        Select a.agency_name, SUM(DATEDIFF(r.return_date, r.rental_date) * c.price_per_day) as Revenue
        From cars c
        Join rentals r on c.car_id = r.car_id
        Join agencies a on c.agency_id = a.agency_id
        Group by a.agency_name
        Order by Revenue Desc
    """
    caltl = spark.sql(query) 
    return caltl

def find_top_5_customers(spark) -> DataFrame:
    query = """
        Select c.customer_name, SUM(DATEDIFF(r.return_date, r.rental_date) * c2.price_per_day) as total_spent
        From rentals r
        Join customers c on r.customer_id = c.customer_id
        Join cars c2 on r.car_id = c2.car_id
        Group by c.customer_name
        Order by total_spent DESC
    """
    t5c = spark.sql(query)
    return t5c

def get_avg_age_customers_by_city(spark) -> DataFrame:
    query = """
        Select c.city, AVG(c.age) as avg_age
        From customers c
        Join rentals r on c.customer_id = r.customer_id
        Group by c.city
        Order by avg_age DESC
        Limit 10
    """
    avg_abc = spark.sql(query)
    return avg_abc 

def find_car_models_most_revenue(spark) -> DataFrame:
    query = """
        Select c.car_name, SUM(DATEDIFF(r.return_date, r.rental_date) * c.price_per_day) as profit
        From cars c
        Join rentals r on c.car_id = r.car_id
        Group by c.car_name
        Order by profit DESC
    """
    cmmr = spark.sql(query)
    return cmmr

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
    get_avg_age_customers_by_city(spark).show(n=10, truncate=False)
    find_car_models_most_revenue(spark).show(n=5, truncate=False)
