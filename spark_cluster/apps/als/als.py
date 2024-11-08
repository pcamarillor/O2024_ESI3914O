from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import explode

# Start Spark session
spark = SparkSession.builder \
            .appName("ASL-Example") \
            .config("spark.ui.port","4040") \
            .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Sample user-song interaction data
data = [
    Row(userId=1, songId=1, rating=4),
    Row(userId=1, songId=2, rating=5),
    Row(userId=2, songId=2, rating=3),
    Row(userId=2, songId=3, rating=4),
    Row(userId=3, songId=1, rating=2),
    Row(userId=3, songId=3, rating=5),
]

# Create DataFrame for interactions
interactions = spark.createDataFrame(data)

# Song metadata (optional)
songs = [
    Row(songId=1, title="Song A"),
    Row(songId=2, title="Song B"),
    Row(songId=3, title="Song C"),
]

songs_df = spark.createDataFrame(songs)

# Show songs df
print("Songs DF")
songs_df.show(truncate=False)

# Show training df
print("Ratings DF")
interactions.show(truncate=False)

# Configure ALS model
als = ALS(
    userCol="userId", 
    itemCol="songId", 
    ratingCol="rating", 
    maxIter=10, 
    regParam=0.1, 
    rank=5, # Controls the dimensionality of the latent vector space for users and items.
    coldStartStrategy="drop"  # Avoids NaN predictions
)

# Train the model
model = als.fit(interactions)

# Generate recommendations for each user
user_recommendations = model.recommendForAllUsers(numItems=3)

# Show recommendations
user_recommendations.show(truncate=False)

# Explode recommendations for easier reading
recommendations = user_recommendations.select("userId", explode("recommendations").alias("rec"))
recommendations = recommendations.join(songs_df, recommendations.rec.songId == songs_df.songId).select("userId", "title", "rec.rating")

# Show user-song recommendations with titles
recommendations.show(truncate=False)


# Generate predictions on the test set
predictions = model.transform(interactions)
predictions.show(truncate=False)

# Set up evaluator to compute RMSE
evaluator = RegressionEvaluator(
    metricName="rmse", 
    labelCol="rating", 
    predictionCol="prediction"
)

# Calculate RMSE
rmse = evaluator.evaluate(predictions)
print(f"Root-mean-square error (RMSE) = {rmse}")