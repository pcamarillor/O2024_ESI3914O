# Import necessary modules
from pyspark.sql import SparkSession
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import StructType, StructField, FloatType

# Initialize SparkSession
spark = SparkSession.builder \
            .appName("Decision-Trees-Example") \
            .config("spark.ui.port","4040") \
            .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Create a small dataset as a list of tuples
# Format: (label, feature1, feature2)
data = [
    (0, 1.0, 0.5),
    (1, 2.0, 1.5),
    (0, 1.5, 0.2),
    (1, 2.2, 1.0),
    (0, 1.0, -0.3),
    (1, 2.5, 1.0)
]

# Define column names
columns = ["label", "feature1", "feature2"]

# Convert list to a DataFrame
df = spark.createDataFrame(data, columns)

# Assemble the features into a single vector column
assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
data_with_features = assembler.transform(df).select("label", "features")

# Split the data into training and test sets 80% training data and 20% testing data
train, test = data_with_features.randomSplit([0.8, 0.2], seed=13)

# Show the whole dataset
print("Dataset")
data_with_features.show()

# Print train dataset
print("train set")
train.show()

# Initialize and train the Decision Tree model
dt = DecisionTreeClassifier(labelCol="label", featuresCol="features")

# ============================
# TRAIN
# ============================

# Train to get the model
dt_model = dt.fit(train)

# Display model summary
print("Decision Tree model summary:{0}".format(dt_model.toDebugString))

# ============================
# PREDICTIONS
# ============================

# Use the trained model to make predictions on the test data
predictions = dt_model.transform(test)

# Show predictions
predictions.show()

# Evaluate the model using MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")

# Calculate accuracy
accuracy = evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"})
print(f"Accuracy: {accuracy}")

# Calculate precision
precision = evaluator.evaluate(predictions, {evaluator.metricName: "weightedPrecision"})
print(f"Precision: {precision}")

# Calculate recall
recall = evaluator.evaluate(predictions, {evaluator.metricName: "weightedRecall"})
print(f"Recall: {recall}")

# Calculate F1 score
f1 = evaluator.evaluate(predictions, {evaluator.metricName: "f1"})
print(f"F1 Score: {f1}")

# Stop Spark session
spark.stop()