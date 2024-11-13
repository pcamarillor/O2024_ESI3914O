from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import Row
from pyspark.ml.feature import VectorAssembler

# Initialize SparkSession
spark = SparkSession.builder \
            .appName("Kmeans-Example") \
            .config("spark.ui.port","4040") \
            .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Sample data in Python (e.g., 2D points with additional dimensions for demonstration)
data = [
    (0, 1.0, 1.0),
    (1, 2.0, 1.0),
    (2, 4.0, 5.0),
    (3, 5.0, 5.0),
    (4, 10.0, 10.0),
    (5, 12.0, 11.0)
]

# Convert data to a DataFrame with column names
df = spark.createDataFrame(data, ["id", "x", "y"])


# Use VectorAssembler to combine "x" and "y" into a single "features" vector column
assembler = VectorAssembler(inputCols=["x", "y"], outputCol="features")
assembled_df = assembler.transform(df)


# Initialize KMeans
k = 2 # Number of clusters
kmeans = KMeans().setK(2).setSeed(1)
model = kmeans.fit(assembled_df)

# Make predictions
predictions = model.transform(assembled_df)

# Evaluate clustering by computing Silhouette score
evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(predictions)
print(f"Silhouette score: {silhouette}")

# Show the result
print("Cluster Centers: ")
for center in model.clusterCenters():
    print(center)

# Stop the Spark session
spark.stop()