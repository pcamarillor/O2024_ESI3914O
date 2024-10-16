from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.streaming import StreamingQueryListener

# Initialize SparkSession
spark = SparkSession.builder \
            .appName("Structured-Streaming-Files-Example") \
            .config("spark.ui.port", "4040") \
            .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "5")


spark.conf.set("spark.sql.streaming.maxFilesPerTrigger", "2")


schema = StructType([
    StructField("student_id", IntegerType(), True),
    StructField("student_first_name", StringType(), True),
    StructField("student_last_name", StringType(), True),
    StructField("gpa", FloatType(), True)
])


students_df = spark.readStream \
            .format("json") \
            .schema(schema) \
            .option("cleanSource", "archive") \
            .option("sourceArchiveDir", "/opt/spark-data/streaming_archive/") \
            .load("/opt/spark-data/streaming_examples/")


team_member_names = ["Uri", "Dyon", "Michelle"]  
students_df = students_df.filter((students_df.gpa > 3) & (~students_df.student_first_name.isin(team_member_names)))


class MyListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Query started: {event.id}")

    
    def onQueryProgress(self, event):
        print(f"batchId: {event.progress.batchId}, numInputRows: {event.progress.numInputRows}")
    
    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")


spark.streams.addListener(MyListener())


query = students_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .trigger(processingTime='3 seconds') \
        .start()

query.awaitTermination(20)
