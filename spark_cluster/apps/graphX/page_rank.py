from pyspark.sql import SparkSession
from graphframes import GraphFrame
import random

# Initialize SparkSession
spark = SparkSession.builder \
            .appName("GraphX-Example") \
            .config("spark.ui.port","4040") \
            .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "5")

# Generate random vertices
size = 13
vertices = spark.read.text("/opt/spark-data/sample_movielens_ratings.txt")

vertices = vertices.withColumn("userID", split(vertices["value"], "::").getItem(0)) \
.withColumn("movieID", split(vertices["value"], "::").getItem(1)) \
.withColumn("rating", split(vertices["value"], "::").getItem(2)) \
.withColumn("timestamp", split(vertices["value"], "::").getItem(3))

vertices = vertices.drop("value")
vertices = vertices.drop("timestamp")
vertices = vertices.drop("rating")

vertices = vertices.withColumn("userID", vertices["userID"].cast("int"))
vertices = vertices.withColumn("movieID", vertices["movieID"].cast("int"))
vertices.show(n=size)

edges = vertices.select(vertices["userID"].alias("src"), vertices["movieID"].alias("dst"))
edges = edges.withColumn("src", edges["src"].cast("int"))
edges = edges.withColumn("dst", edges["src"].cast("int"))

print("Edges as DataFrame:")
edges.show(n=20)

# Create a GraphFrame
graph = GraphFrame(vertices, edges)
print("GraphFrame:")
graph.vertices.show()
graph.edges.show(n=size, truncate=False)

# Query: Count the number of "married" connections in the graph.
number_follow = graph.edges.filter("relationship = 'married'").count()
print("Number of 'married' connections in the graph: {0}".format(number_follow))

pr = graph.pageRank(resetProbability=0.85, maxIter=20)
pr.vertices.select("id", "pagerank").orderBy("pagerank", ascending=False).show()

spark.stop()


