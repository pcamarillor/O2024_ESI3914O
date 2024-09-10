from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Initialize Spark session
spark = SparkSession.builder.appName("MemoryOptimized").getOrCreate()

# Sample data: Large collection
data = [("John", 28, 50000), 
        ("Jane", 35, 80000), 
        ("Doe", 22, 45000), 
        ("Alice", 29, 75000), 
        ("Bob", 40, 90000)]

# Create a DataFrame
columns = ["Name", "Age", "Salary"]
df = spark.createDataFrame(data, schema=columns)

# Optimized pipeline (memory-efficient)
# 1. Filter out people under 30 first (reducing the dataset early)
filtered_df = df.filter(col("Age") > 30)

# 2. Select only the Salary column (reducing the number of columns in memory)
salary_df = filtered_df.select("Salary")

# 3. Calculate the average salary
avg_salary = salary_df.agg(avg(col("Salary"))).first()[0]

print(f"Average salary of people older than 30: {avg_salary}")

# Stop the Spark session
spark.stop()
