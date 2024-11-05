# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LinearSVC
from pyspark.sql.types import StructType, StructField, FloatType
from pyspark.sql import Row

# Initialize SparkSession
spark = SparkSession.builder \
            .appName("SVM-Example") \
            .config("spark.ui.port","4040") \
            .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

data = [
    (1.0, 1.0, 2.0),
    (1.0, 2.0, 3.0),
    (0.0, -1.0, -2.0),
    (0.0, -2.0, -3.0)
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

# Initialize and configure the LinearSVC model
lsvc = LinearSVC(maxIter=10, regParam=0.01)

# Fit the model
model = lsvc.fit(data_with_features)

# Make predictions
predictions = model.transform(data_with_features)
predictions.show()

# Print model details
print("Coefficients: " + str(model.coefficients))

# Stop the Spark session
spark.stop()