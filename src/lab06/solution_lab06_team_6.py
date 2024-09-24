from pyspark.sql import DataFrame

def register_data_frames(cas_df, brands_df, customers_df, agencies_df, rentals_df):
    cas_df.createOrReplaceTempView('cars')
    brands_df.createOrReplaceTempView('brands')
    customers_df.createOrReplaceTempView('customers')
    agencies_df.createOrReplaceTempView('agencies')
    rentals_df.createOrReplaceTempView('rentals')

    return None

def get_three_most_popular_car_brands(spark) -> DataFrame:
    #brand_id,brand_name,country
    #car_id,car_name,brand_id,price_per_day,agency_id

    query = """
    SELECT b.brand_name, COUNT(r.rental_id) as rental_count
    FROM rentals r
    JOIN cars c ON r.car_id = c.car_id
    JOIN brands b ON c.brand_id = b.brand_id
    GROUP BY b.brand_name
    ORDER BY rental_count DESC
    LIMIT 3
    """
    return spark.sql(query)


def calculate_total_revenue(spark) -> DataFrame:
    #agency_id,agency_name,city
    #rental_id,customer_id,car_id,rental_date,return_date
    #car_id,car_name,brand_id,price_per_day,agency_id

    query = """
    SELECT a.agency_name, SUM(DATEDIFF(r.return_date, r.rental_date) * c.price_per_day) as total_revenue
    FROM rentals r
    JOIN cars c ON r.car_id = c.car_id
    JOIN agencies a ON c.agency_id = a.agency_id 
    GROUP BY a.agency_name
    ORDER BY total_revenue DESC
    """
    return spark.sql(query)

def find_top_5_customers(spark) -> DataFrame:
    result = spark.sql("""
                        SELECT C.customer_name, 
                               SUM(DATEDIFF(R.return_date, R.rental_date) * CA.price_per_day) AS total_spent
                        FROM customers C
                        JOIN rentals R ON C.customer_id = R.customer_id
                        JOIN cars CA ON CA.car_id = R.car_id
                        GROUP BY C.customer_name
                        ORDER BY total_spent DESC
                        LIMIT 5
                        """)
    return result

def get_avg_age_customers_by_city(spark) -> DataFrame:
    result = spark.sql("""
    SELECT customers.city, AVG(customers.age) AS average_age
    FROM rentals
    JOIN customers ON rentals.customer_id = customers.customer_id
    GROUP BY customers.city
    ORDER BY average_age DESC
    LIMIT 10
    """)

    return result

def find_car_models_most_revenue(spark) -> DataFrame:
    """
    Find which car models generate the most revenue. Identify which specific car models (e.g., car name) are the most profitable based on rental duration and price per day. Limit restult to the top 5 car models. Resulting Data Frame should only contain car name and revenue columns.
    """
    query = """
    SELECT c.car_name, SUM(DATEDIFF(r.return_date, r.rental_date) * c.price_per_day) as revenue 
    FROM cars c
    JOIN rentals r 
    ON r.car_id = c.car_id
    GROUP BY c.car_name
    ORDER BY revenue DESC
    LIMIT 5;
    """

    result_df = spark.sql(query)

    return result_df
