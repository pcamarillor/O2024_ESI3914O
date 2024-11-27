from pyspark.sql import SparkSession
from graphframes import GraphFrame
from pyspark.sql.functions import split
import random

spark = SparkSession.builder \
            .appName("GraphX-Example") \
            .config("spark.ui.port", "4040") \
            .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "5")

vertices = spark.read.text("/opt/spark-data/sample_movielens_ratings.txt")
vertices = vertices.withColumn("userID", split(vertices["value"], "::").getItem(0)) \
                   .withColumn("movieID", split(vertices["value"], "::").getItem(1)) \
                   .withColumn("rating", split(vertices["value"], "::").getItem(2)) \
                   .withColumn("timestamp", split(vertices["value"], "::").getItem(3))
vertices = vertices.drop("value", "timestamp", "rating")
vertices = vertices.withColumn("userID", vertices["userID"].cast("int")) \
                   .withColumn("movieID", vertices["movieID"].cast("int"))

user_vertices = vertices.select(vertices["userID"].alias("id")).distinct()
movie_vertices = vertices.select(vertices["movieID"].alias("id")).distinct()
all_vertices = user_vertices.union(movie_vertices)
print("Vertices as DataFrame:")
all_vertices.show()

vertices_data = all_vertices.collect()

connections = []
for i in range(len(vertices_data)):
    num_connections = random.randint(1, min(5, len(vertices_data) - 1))
    for _ in range(num_connections):
        target_node = random.choice([n for n in vertices_data if n != vertices_data[i]])
        connection_type = random.choice(["rated", "liked", "watched", "recommended"])
        connections.append((vertices_data[i][0], target_node[0], connection_type))

edges = spark.createDataFrame(connections, ["src", "dst", "relationship"])
print("Edges as DataFrame:")
edges.show(n=20)

all_vertices = all_vertices.dropDuplicates(["id"])
graph = GraphFrame(all_vertices, edges)

graph = GraphFrame(all_vertices, edges)
print("GraphFrame:")
graph.vertices.show()
graph.edges.show(truncate=False)

number_liked = graph.edges.filter("relationship = 'liked'").count()

pr = graph.pageRank(resetProbability=0.85, maxIter=20)
pr.vertices.select("id", "pagerank").orderBy("pagerank", ascending=False).show()

spark.stop()
