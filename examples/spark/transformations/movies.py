from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Movies-Activity").getOrCreate()

df_movies = spark.read \
            .option("inferSchema", "true") \
            .json("../../../datasets/movies.json")

df_movies.printSchema()
df_movies.show(n=7)


spark.stop()