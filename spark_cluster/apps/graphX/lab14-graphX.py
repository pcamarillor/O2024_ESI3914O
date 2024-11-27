# lab14 team9

from pyspark.sql import SparkSession
from graphframes import GraphFrame

spark = SparkSession.builder \
    .appName("MovieInteractionAnalysis") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.3-spark3.5-s_2.13") \
    .getOrCreate()

data = spark.read.csv("/opt/spark-data/sample_movielens_ratings.txt", sep="::", inferSchema=True)
data = data.withColumnRenamed("_c0", "user") \
           .withColumnRenamed("_c1", "movie") \
           .withColumnRenamed("_c2", "rating")

vertices = data.select("movie").distinct().withColumnRenamed("movie", "id")
edges = data.select("user", "movie").withColumnRenamed("user", "src").withColumnRenamed("movie", "dst")

graph = GraphFrame(vertices, edges)

pr = graph.pageRank(resetProbability=0.85, maxIter=20)
pr.vertices.orderBy("pagerank", ascending=False).show()

spark.stop()