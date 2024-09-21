from pyspark.sql import DataFrame


def union_cars(cars_df_1, cars_df_2, cars_df_3) -> DataFrame:
    """
    Task 1: Combine three car datasets. This method should support the case when
    there's mismatch in the schema definition or missing columns
    """
    # Cars A: car_id,car_name,brand_id,price_per_day,agency_id
    # Cars B: brand_id,price_per_day,car_id,car_name,agency_id
    # Cars C: car_name,agency_id,brand_id,car_id

    # Cars A: car_id,car_name,brand_id,price_per_day,agency_id
    # Cars B: car_id,car_name,brand_id,price_per_day,agency_id
    # Cars C: car_id,car_name,brand_id,null         ,agency_id

    return cars_df_1.unionByName(cars_df_2).unionByName(
        cars_df_3, allowMissingColumns=True
    )


def distinct_cities(agencies_df, customers_df) -> DataFrame:
    """
    Task 2: Combine the city from the Agencies table with the city from
    the Customers table to get all unique cities involved in car rentals.
    """

    # Agencies: agency_id,agency_name,city
    # Customers: customer_id,customer_name,city

    return agencies_df.select("city").union(customers_df.select("city")).distinct()


def left_join_cars_brands(cars_df, brands_df) -> DataFrame:
    """
    Task 3: Perform a left join between Cars and Brands.
    """

    # Cars: car_id,car_name,brand_id,price_per_day,agency_id
    # Brands: brand_id,brand_name

    return cars_df.join(brands_df, cars_df.brand_id == brands_df.brand_id, how="left")


def right_join_cars_rentals(rentals_df, cars_df) -> DataFrame:
    """
    Task 4: Perform a right join between Cars and Rentals
    """

    # Rentals: rental_id,car_id,customer_id,rental_start_date,rental_end_date
    # Cars: car_id,car_name,brand_id,price_per_day,agency_id

    return cars_df.join(rentals_df, rentals_df.car_id == cars_df.car_id, how="right")
