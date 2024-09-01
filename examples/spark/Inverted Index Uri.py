from pyspark.sql import SparkSession

# Configuración de SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ITESO-2024-SparkIntroduction") \
    .config("spark.driver.bindAddress", "localhost") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

sc = spark.sparkContext

# Crear un RDD con una lista de oraciones
sentences = [
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
    "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
    "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.",
    "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.",
    "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.",
    "Curabitur pretium tincidunt lacus. Nulla gravida orci a odio.",
    "Nullam varius, turpis et commodo pharetra, est eros bibendum elit, nec malesuada elit elit vel lectus.",
    "Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium.",
    "Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit.",
    "Sed quia consequuntur magni dolores eos qui ratione voluptatem sequi nesciunt.",
    "Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit.",
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
    "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat."
]

# Paralelizar las oraciones en un RDD
sentences_rdd = sc.parallelize(sentences)

# Tokenizar las oraciones en palabras
words_rdd = sentences_rdd.flatMap(lambda line: line.split())
print(words_rdd.collect())

# Calcular la frecuencia de cada palabra
word_counts_rdd = words_rdd.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
print(word_counts_rdd.collect())

# Encontrar la palabra más común
most_common_word = word_counts_rdd.takeOrdered(1, key=lambda x: -x[1])
print("Most common word:", most_common_word)

# Calcular la longitud promedio de las palabras
total_length_rdd = words_rdd.map(lambda word: len(word)).reduce(lambda a, b: a + b)
total_words = words_rdd.count()
average_word_length = total_length_rdd / total_words if total_words > 0 else 0
print("Average word length:", average_word_length)

# Encontrar la palabra más larga y más corta usando sortBy
small = words_rdd.sortBy(lambda word: len(word)).first()
long = words_rdd.sortBy(lambda word: -len(word)).first()

print("Shortest word:", small)
print("Longest word:", long)

# Detener el SparkContext
sc.stop()