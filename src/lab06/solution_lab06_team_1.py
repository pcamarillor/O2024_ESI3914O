from pyspark.sql import DataFrame

def register_data_frames(cars_df, brands_df, customers_df, agencies_df, rentals_df):

    cars_df.createOrReplaceTempView("cars")
    brands_df.createOrReplaceTempView("brands")
    customers_df.createOrReplaceTempView("customers")
    agencies_df.createOrReplaceTempView("agencies")
    rentals_df.createOrReplaceTempView("rentals")

    return None

def get_three_most_popular_car_brands(spark) -> DataFrame:
    # Brand_id, brand_name, car_id, rental_count
   # cars, rentals, brands
   #result = spark.sql("SELECT * FROM brands")
   brand_with_car_id = spark.sql("SELECT b.brand_id, b.brand_name, c.car_id FROM brands b JOIN cars c ON c.brand_id = b.brand_id")
   brand_with_car_id.createOrReplaceTempView("brand_with_car_id")
   brand_with_car_id.show()
   rentals_with_brand = spark.sql("SELECT b.brand_name, count(*) AS rental_count FROM rentals r JOIN brand_with_car_id b ON b.car_id = r.car_id GROUP BY b.brand_name ORDER BY rental_count DESC LIMIT 3")
   rentals_with_brand.show()
   return rentals_with_brand

def calculate_total_revenue(spark) -> DataFrame:
    result = spark.sql("""
        SELECT 
            a.agency_name AS agency_name,
            SUM(DATEDIFF(r.return_date,r.rental_date) * c.price_per_day) AS total_revenue
        FROM rentals r
        JOIN cars c ON r.car_id = c.car_id
        JOIN agencies a ON c.agency_id = a.agency_id
        GROUP BY a.agency_name
        ORDER BY total_revenue DESC
    """)
    return result 

def find_top_5_customers(spark) -> DataFrame:
    result = spark.sql("""
        SELECT cu.customer_name,
               SUM((ca.price_per_day * DATEDIFF(TO_DATE(r.return_date), TO_DATE(r.rental_date)))) AS total_spent
        FROM rentals r
        JOIN cars ca ON r.car_id = ca.car_id
        JOIN customers cu ON r.customer_id = cu.customer_id
        GROUP BY cu.customer_name
        ORDER BY total_spent DESC
        LIMIT 5
    """)
    return result 

def get_avg_age_customers_by_city(spark) -> DataFrame:
    result = spark.sql("""
    SELECT 
        customers.city, 
        AVG(customers.age) AS avg_age
    FROM rentals r JOIN customers 
        ON r.customer_id = customers.customer_id
    GROUP BY customers.city
    ORDER BY avg_age DESC
    LIMIT 10
    """)

    return result

def find_car_models_most_revenue(spark) -> DataFrame:
    result = spark.sql("""
        SELECT 
            c.car_name AS car_name,
            SUM(DATEDIFF(r.return_date, r.rental_date) * c.price_per_day) AS revenue
        FROM rentals r
        JOIN cars c ON r.car_id = c.car_id
        GROUP BY c.car_name
        ORDER BY revenue DESC
        LIMIT 5
    """)
    
    return result
