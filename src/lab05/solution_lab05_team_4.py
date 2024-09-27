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

