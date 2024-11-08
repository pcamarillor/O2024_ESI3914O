# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import LinearSVC, OneVsRest
from pyspark.sql.types import StructType, StructField, FloatType
from pyspark.sql import Row
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


# Initialize SparkSession
spark = SparkSession.builder \
            .appName("MultiClassSVM-Example") \
            .config("spark.ui.port","4040") \
            .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Generate sample multi-class dataset
data = [
    (0.0, 1.0, 2.0),
    (0.0, 2.0, 1.5),
    (0.0, 2.1, 3.0),
    (1.0, -1.0, -2.0),
    (1.0, -2.0, -1.5),
    (1.0, -2.5, -3.5),
    (2.0, 3.0, 4.0),
    (2.0, 4.0, 3.5),
    (2.0, 3.5, 5.0),
    (0.0, 1.5, 1.7),
    (0.0, 1.8, 2.5),
    (1.0, -1.5, -1.8),
    (1.0, -2.2, -2.5),
    (2.0, 3.2, 4.1),
    (2.0, 4.1, 4.5),
    (0.0, 2.2, 2.8),
    (1.0, -1.2, -1.6),
    (2.0, 3.6, 3.8),
    (2.0, 4.3, 5.1)
]

# Define schema for the DataFrame
schema = StructType([
    StructField("label", FloatType(), True),
    StructField("feature1", FloatType(), True),
    StructField("feature2", FloatType(), True)
])

# Convert data to DataFrame
df = spark.createDataFrame(data, schema=schema)

# Transform features into a single vector column
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
df = assembler.transform(df).select("label", "features")

# Split the data into training and testing sets (80% training, 20% testing)
train_df, test_df = df.randomSplit([0.8, 0.2], seed=47)

# Initialize the LinearSVC classifier for binary classification
lsvc = LinearSVC(maxIter=10, regParam=0.01)

# Set up OneVsRest classifier for multi-class classification
ovr = OneVsRest(classifier=lsvc)

# Train the model
ovr_model = ovr.fit(train_df)

# Make predictions on the test set
predictions = ovr_model.transform(test_df)

# Show predictions
predictions.show()

# Evaluate the model
evaluator = MulticlassClassificationEvaluator(metricName="f1")
f1 = evaluator.evaluate(predictions)
print("Test F1 Score = ", f1)

# Stop the Spark session
spark.stop()