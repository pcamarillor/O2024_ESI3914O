from pyspark.sql import SparkSession
from pyspark.sql.functions import year  # Import the year function

# Initialize Spark session
spark = SparkSession.builder.appName("Movies-Activity").getOrCreate()

# Load the movies dataset
df_movies = spark.read \
            .option("inferSchema", "true") \
            .json("../../../datasets/movies.json")

# Print the schema to verify the structure of the dataset
df_movies.printSchema()

# Display the first 7 rows of the data
df_movies.show(n=7)

# Extract the year from the Release_Date column
df_movies_with_year = df_movies.withColumn("year", year(df_movies['Release_Date']))

# Filter movies released after 2000 with an MPAA rating of 'R'
df_filtered = df_movies_with_year.filter((df_movies_with_year['year'] > 2000) & (df_movies_with_year['MPAA_Rating'] == 'R'))

# Show the filtered data
df_filtered.show()

# Stop the Spark session
spark.stop()
