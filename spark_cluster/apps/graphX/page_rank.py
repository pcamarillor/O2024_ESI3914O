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
vertices_data = [(str(i), "User " + str(i)) for i in range(size)]
vertices = spark.createDataFrame(vertices_data, ["id", "name"])
vertices.show(n=size)

# Generate random connections
connections = []
for i in range(len(vertices_data)):
    num_connections = random.randint(1, min(5, len(vertices_data) - 1))  # Ensure at least one connection
    for _ in range(num_connections):
        target_node = random.choice([n for n in vertices_data if n != vertices_data[i]])
        connection_type = random.choice(["friend", "married", "knows", "partner"])
        connections.append((vertices_data[i][0], target_node[0], connection_type))

edges = spark.createDataFrame(connections, ["src", "dst", "relationship"])
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


