# Import necessary modules
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import StructType, StructField, FloatType

# Initialize SparkSession
spark = SparkSession.builder \
            .appName("Structured-Streaming-Files-Example") \
            .config("spark.ui.port","4040") \
            .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Create a small dataset as a list of tuples
# Format: (label, feature1, feature2)
data = [
    (1.0, 2.0, 3.0),
    (0.0, 1.0, 2.5),
    (1.0, 3.0, 5.0),
    (0.0, 0.5, 1.0),
    (1.0, 4.0, 6.0)
]

# Define schema for the DataFrame
schema = StructType([
    StructField("label", FloatType(), True),
    StructField("feature1", FloatType(), True),
    StructField("feature2", FloatType(), True)
])

# Convert list to a DataFrame
df = spark.createDataFrame(data, schema=schema)

# Assemble the features into a single vector column
assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
data_with_features = assembler.transform(df).select("label", "features")

# Split the data into training and test sets 80% training data and 20% testing data
train, test = data_with_features.randomSplit([0.8, 0.2], seed=57)

# Show the whole dataset
print("Dataset")
data_with_features.show()

# Print train dataset
print("train set")
train.show()

# Create a logistic regression model
lr = LogisticRegression(maxIter=10, regParam=0.01)

# ============================
# TRAIN
# ============================

# Train to get the model
lr_model = lr.fit(train)

# Print coefficients
print("Coefficients: " + str(lr_model.coefficients))

# Display model summary
training_summary = lr_model.summary

# ============================
# PREDICTIONS
# ============================

# Use the trained model to make predictions on the test data
predictions = lr_model.transform(test)

# Show predictions
predictions.select("features", "prediction", "probability").show()

# Stop Spark session
spark.stop()