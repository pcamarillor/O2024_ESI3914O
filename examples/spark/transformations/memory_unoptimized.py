from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Initialize Spark session
spark = SparkSession.builder.appName("MemoryUnoptimized").getOrCreate()

'''spark = SparkSession.builder \
    .appName("MemoryOptimized") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()'''

# Sample data: Large collection (this could represent a much larger dataset)
data = [("John", 28, 50000), 
        ("Jane", 35, 80000), 
        ("Doe", 22, 45000), 
        ("Alice", 29, 75000), 
        ("Bob", 40, 90000)]

# Create a DataFrame
columns = ["Name", "Age", "Salary"]
df = spark.createDataFrame(data, schema=columns)
df.show()

# Unoptimized pipeline (keeping too much data in memory)
# 1. Select Name, Age, Salary for all people
selected_df = df.select("Name", "Age", "Salary")

# 2. Filter out people under 30 (but unnecessary columns are still in memory)
filtered_df = selected_df.filter(col("Age") > 30)

# 3. Calculate the average salary
avg_salary = filtered_df.agg(avg(col("Salary"))).first()[0]

print(f"Average salary of people older than 30: {avg_salary}")

# Stop the Spark session
spark.stop()
