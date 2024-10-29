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

# Transform features into a single "features" column as a Dense Vector
assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
data_with_features = assembler.transform(df).select("label", "features")

# Initialize logistic regression model
lr = LogisticRegression(maxIter=10, regParam=0.01)

# Fit the model
lr_model = lr.fit(data_with_features)

# Display model summary
training_summary = lr_model.summary

# ============================
# PREDICTIONS
# ============================

# Create new data for predictions
new_data = [
    (2.5, 3.5),
    (1.5, 2.0),
    (3.0, 4.5),
    (0.0, 1.0)
]

# Define schema for prediction DataFrame (no label column required here)
new_schema = StructType([
    StructField("feature1", FloatType(), True),
    StructField("feature2", FloatType(), True)
])

# Convert list to a DataFrame
predictions_df = spark.createDataFrame(new_data, schema=new_schema)

# Transform new features into a single "features" column
new_data_with_features = assembler.transform(predictions_df).select("features")

# Use the trained model to make predictions on the new data
predictions = lr_model.transform(new_data_with_features)

# Show predictions
predictions.select("features", "prediction", "probability").show()

# Stop Spark session
spark.stop()