from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Movies-Activity").getOrCreate()

df_movies = spark.read \
            .option("inferSchema", "true") \
            .json("../../../datasets/movies.json")

# df_movies.printSchema()
# df_movies.show(n=7)

df_movies_filtered = df_movies.filter(df_movies.IMDB_Votes.isNotNull() & df_movies.Major_Genre.isNotNull())
imdb_avg_by_genre = df_movies_filtered.groupBy("Major_Genre").avg("IMDB_Rating")

imdb_avg_by_genre.show(n=20)

spark.stop()