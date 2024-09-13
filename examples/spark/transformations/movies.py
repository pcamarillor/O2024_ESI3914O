from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col 

#spark = SparkSession.builder.appName("Movies-Activity").getOrCreate()
spark = SparkSession.builder \
    .appName("Movies-Activity") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

df_movies = spark.read \
            .option("inferSchema", "true") \
            .json("datasets/movies.json")

df_movies.printSchema()
#df_movies.show(n=7)

def total_us_gross_by_distributor(df_movies):
   #Clean null values
    df_clean = df_movies.na.drop(subset=["US_Gross", "Distributor"])
    total_us_gross = df_clean.groupBy("Distributor") \
                             .agg({"US_Gross": "sum"}) \
                             .withColumnRenamed("sum(US_Gross)", "Total_US_Gross") \
                             .orderBy("Total_US_Gross", ascending=False)
    
    return total_us_gross


#total_us_gross_by_distributor(df_movies).show()

spark.stop()