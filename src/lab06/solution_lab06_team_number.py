from pyspark.sql import DataFrame

def register_data_frames(cars_df, brands_df, customers_df, agencies_df, rentals_df):
    cars_df.createOrReplaceTempView("cars")
    brands_df.createOrReplaceTempView("brands")
    customers_df.createOrReplaceTempView("customers")
    agencies_df.createOrReplaceTempView("agencies")
    rentals_df.createOrReplaceTempView("rentals")
    return None

def get_three_most_popular_car_brands(spark) -> DataFrame:
    spark.sql("SELECT b.brand_name, COUNT(r.rental_count) \
        FROM cars c  \
        JOIN brands b \
        ON c.brand_id = b.brand_id \
        JOIN rentals r \
        ON c.car_id = r.car_id \
        GROUP BY b.brand_name \
        ORDER BY DESC").show(n=5, truncate=False)
    return None

def calculate_total_revenue(spark) -> DataFrame:
    spark.sql("SELECT a.agency_name, SUM(r.renevue) \
        FROM cars c  \
        JOIN agencies a \
        ON c.agency_id = a.agency_id \
        JOIN rentals r \
        ON c.car_id = r.car_id \
        GROUP BY b.brand_name \
        ORDER BY DESC").show(n=5, truncate=False)
    return None 

def find_top_5_customers(spark) -> DataFrame: # usar DATEDIFF()
    spark.sql("SELECT c.customer_name, \
        c.price_per_day * (r.return_date - r.rental_date) AS total_spent \ 
        FROM customers c  \
        JOIN rentals r \
        ON c.customer_id = r.customer_id \
        JOIN cars c2 \
        ON r.car_id = c2.car_id \
        GROUP BY c.customer_name \
        ORDER BY DESC").show(n=5, truncate=False)
    return None 

def get_avg_age_customers_by_city(spark) -> DataFrame:
    return None 

def find_car_models_most_revenue(spark) -> DataFrame:
    return None 
