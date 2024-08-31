from pyspark.sql import SparkSession


#Lo que nos permite trabajar con spark
spark = SparkSession.builder \
    .appName("Lecture-06-ReduceByKey") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "localhost") \
    .config("spark.ui.port","4040") \
    .getOrCreate()


sc = spark.sparkContext

documents = [
(1, "Spark is an open-source distributed computing system"),
(2, "MapReduce is a programming model for large-scale data processing"),
(3, "Spark provides an API for distributed data processing")
]

#convierte la lista de documentos en un RDD
rdd = sc.parallelize(documents)

#RDD en pares palabra y doc
palabra_rdd = rdd.flatMap(lambda doc: [(word, doc[0]) for word in doc[1].split()])

#elminar duplicados
duplicados = palabra_rdd.distinct()

#agrupar por palabra y los Ids
groupword = duplicados.groupByKey()

#agrupar en listas
finalrdd = groupword.mapValues(list)

result = finalrdd.collect()
for word, lista in result:
    print(f"{word}: {lista}")

while True:
    pass

spark.stop()

