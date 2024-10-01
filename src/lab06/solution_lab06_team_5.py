from pyspark.sql import DataFrame

def register_data_frames(cars_df, brands_df, customers_df, agencies_df, rentals_df):
    cars_df.createOrReplaceTempView("cars")
    brands_df.createOrReplaceTempView("brands")
    customers_df.createOrReplaceTempView("customers")
    agencies_df.createOrReplaceTempView("agencies")
    rentals_df.createOrReplaceTempView("rentals")

def get_three_most_popular_car_brands(spark) -> DataFrame:
    df = spark.sql("SELECT b.brand_name, COUNT(r.rental_id) AS rental_count \
        FROM rentals r \
        JOIN cars c ON r.car_id = c.car_id \
        JOIN brands b ON c.brand_id = b.brand_id \
        GROUP BY b.brand_name \
        ORDER BY rental_count DESC \
        LIMIT 3")

    df.show(n=5, truncate=False)
    return df

def calculate_total_revenue(spark) -> DataFrame:
    df = spark.sql("SELECT a.agency_name, SUM(c.price_per_day * DATEDIFF(r.return_date, r.rental_date)) AS total_revenue \
        FROM rentals r \
        JOIN cars c ON r.car_id = c.car_id \
        JOIN agencies a ON c.agency_id = a.agency_id \
        GROUP BY a.agency_name \
        ORDER BY total_revenue DESC \
        LIMIT 5")

    df.show(truncate=False)
    return df

def find_top_5_customers(spark) -> DataFrame:
    df = spark.sql("SELECT cu.customer_name, SUM(ca.price_per_day * DATEDIFF(r.return_date, r.rental_date)) AS total_spent \
        FROM customers cu \
        JOIN rentals r ON cu.customer_id = r.customer_id \
        JOIN cars ca ON ca.car_id = r.car_id \
        GROUP BY cu.customer_name \
        ORDER BY total_spent DESC \
        LIMIT 5")

    df.show(n=5, truncate=False)
    return df

def get_avg_age_customers_by_city(spark) -> DataFrame:
    df = spark.sql("SELECT c.city, AVG(c.age) AS average_age \
        FROM customers c \
        JOIN rentals r ON c.customer_id = r.customer_id \
        GROUP BY c.city  \
        ORDER BY average_age DESC \
        LIMIT 10")

    df.show(truncate=False)
    return df

def find_car_models_most_revenue(spark) -> DataFrame:
    df = spark.sql("SELECT c.car_name, SUM(c.price_per_day * DATEDIFF(r.return_date, r.rental_date)) AS revenue \
        FROM cars c \
        JOIN rentals r ON c.car_id = r.car_id \
        GROUP BY c.car_name \
        ORDER BY revenue DESC \
        LIMIT 5")

    df.show(truncate=False)
    return df
