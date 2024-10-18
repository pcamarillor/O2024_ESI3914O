from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.streaming import StreamingQueryListener

spark = SparkSession.builder \
    .appName("Structured-Streaming-Files-Example") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "5")

schema = StructType([
    StructField("student_id", IntegerType(), True),
    StructField("student_first_name", StringType(), True),
    StructField("student_last_name", StringType(), True),
    StructField("gpa", FloatType(), True)
])

team_members = []

students_df = spark.readStream \
    .format("json") \
    .option("maxFilesPerTrigger", 2) \
    .option("cleanSource", "archive") \
    .option("sourceArchiveDir", "/opt/spark-data/streaming_archive/") \
    .schema(schema) \
    .load("/opt/spark-data/streaming_examples/")

filtered_students_df = students_df.filter(~col("student_first_name").isin(team_members))

class CustomQueryListener(StreamingQueryListener):
    def onQueryProgress(self, event):
        print(f"Batch ID: {event.progress.id}, Num Input Rows: {event.progress.numInputRows}")

    def onQueryStarted(self, event):
        print(f"Query started: {event.id}")

    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")

spark.streams.addListener(CustomQueryListener())

query = filtered_students_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination(20)
print("Streaming closed")
