from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from graphframes import GraphFrame

spark = SparkSession.builder \
    .appName("ITESO Movie PageRank") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "5")

ratings_path = "/opt/spark-data/sample_movielens_ratings.txt"
ratings_df = spark.read.option("delimiter", "::").csv(ratings_path, inferSchema=True, header=False)
ratings_df = ratings_df.withColumnRenamed("_c0", "user_id") \
                       .withColumnRenamed("_c1", "movie_id") \
                       .withColumnRenamed("_c2", "rating") \
                       .withColumnRenamed("_c3", "timestamp")

users = ratings_df.select("user_id").distinct().withColumnRenamed("user_id", "id")
movies = ratings_df.select("movie_id").distinct().withColumnRenamed("movie_id", "id")
vertices = users.union(movies)

edges = ratings_df.select(col("user_id").alias("src"), col("movie_id").alias("dst")).distinct()

graph = GraphFrame(vertices, edges)

pr = graph.pageRank(resetProbability=0.85, maxIter=20)
pr_result = pr.vertices.filter(col("id").isin(movies.select("id").rdd.flatMap(lambda x: x).collect())) \
                       .select("id", "pagerank") \
                       .orderBy("pagerank", ascending=False)

pr_result.show(1, truncate=False)

spark.stop()
