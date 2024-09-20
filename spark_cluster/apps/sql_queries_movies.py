from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

spark = SparkSession.builder \
    .appName("Example-SQL-Queries") \
    .config("spark.ui.port","4040") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df_movies = spark.read \
                .option("inferSchema", "true") \
                .json("/opt/spark-data/movies.json")

df_movies.createOrReplaceTempView("movies")

df_movies.printSchema()

result = spark.sql("SELECT Title, MPAA_Rating FROM movies")

result.show(n=5, truncate=False)

spark.sql(
"SELECT Title, IMDB_Rating FROM movies WHERE IMDB_Rating > 8"
).show(n=5, truncate=False)


spark.sql("SELECT Major_Genre, COUNT(*) AS Genre_Count FROM movies GROUP BY Major_Genre") \
.show(n=5)

# Normalizing director column
df_directors = df_movies.select("Director").distinct()
df_directors = df_directors.withColumn("Director_ID", monotonically_increasing_id())
movies_with_director_id = df_movies.join(df_directors, on="Director", how="left").drop("Director")
movies_with_director_id = movies_with_director_id.withColumnRenamed("Director_ID", "Director")

movies_with_director_id.createOrReplaceTempView("movies")
df_directors.createOrReplaceTempView("directors")

spark.sql("SELECT m.Title, d.Director FROM movies m JOIN directors d ON m.Director = d.Director_ID").show(n=5, truncate=False)


spark.sparkContext.stop()