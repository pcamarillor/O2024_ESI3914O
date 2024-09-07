from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("DF Avg Salary Example").getOrCreate()

df = spark.createDataFrame([
    (1, "HR", 50000),
    (2, "IT", 70000),
    (3, "HR", 60000)
], ["id", "department", "salary"])

df.show()
df.printSchema()

avg_salary_df = df.groupBy("department").avg("salary")

avg_salary_df.printSchema()
avg_salary_df.show()

spark.sparkContext.stop()