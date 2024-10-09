from pyspark.sql import DataFrame

def register_data_frames(cars_df, brands_df, customers_df, agencies_df, rentals_df):
    cars_df.createOrReplaceTempView("cars")
    brands_df.createOrReplaceTempView("brands")
    customers_df.createOrReplaceTempView("customers")
    agencies_df.createOrReplaceTempView("agencies")
    rentals_df.createOrReplaceTempView("rentals")

def get_three_most_popular_car_brands(spark) -> DataFrame:
    return spark.sql("""
              SELECT brands.brand_name, COUNT(rentals.rental_id) AS rental_count FROM cars 
              JOIN rentals ON cars.car_id = rentals.car_id 
              JOIN brands ON cars.brand_id = brands.brand_id 
              GROUP BY brands.brand_name 
              ORDER BY rental_count DESC 
              LIMIT 3
              """)

def calculate_total_revenue(spark) -> DataFrame:
    return spark.sql("""
                     SELECT agencies.agency_name, SUM(DATEDIFF(rentals.return_date, rentals.rental_date) * cars.price_per_day) AS total_revenue FROM rentals JOIN cars ON rentals.car_id = cars.car_id 
                     JOIN agencies ON cars.agency_id = agencies.agency_id 
                     GROUP BY agencies.agency_name 
                     ORDER BY total_revenue DESC
                     """) 

def find_top_5_customers(spark) -> DataFrame:
    return spark.sql("""
                     SELECT customers.customer_name, SUM(DATEDIFF(rentals.return_date, rentals.rental_date) * cars.price_per_day) AS total_spent FROM rentals JOIN cars ON rentals.car_id = cars.car_id 
                     JOIN customers ON rentals.customer_id = customers.customer_id 
                     GROUP BY customers.customer_name 
                     ORDER BY total_spent 
                     DESC LIMIT 5
                     """) 

def get_avg_age_customers_by_city(spark) -> DataFrame:
    return spark.sql("""
                     SELECT customers.city, AVG(customers.age) AS average_age 
                     FROM customers 
                     JOIN rentals ON customers.customer_id = rentals.customer_id 
                     GROUP BY customers.city 
                     ORDER BY average_age DESC 
                     LIMIT 10
                     """) 

def find_car_models_most_revenue(spark) -> DataFrame:
    return spark.sql("""
                     SELECT cars.car_name, SUM(DATEDIFF(rentals.return_date, rentals.rental_date) * cars.price_per_day) AS revenue 
                     FROM rentals 
                     JOIN cars ON rentals.car_id = cars.car_id 
                     GROUP BY cars.car_name 
                     ORDER BY revenue DESC 
                     LIMIT 5
                     """) 
