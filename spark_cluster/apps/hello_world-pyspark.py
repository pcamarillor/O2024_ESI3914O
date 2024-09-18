from pyspark.sql import SparkSession

# Iniciar la sesión de Spark
spark = SparkSession.builder \
    .appName("ITESO-BigData-Hello-World-App") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Verificar que la sesión Spark está conectada al master correcto
print("Spark Master URL:", sc.master)
print("Hello World App - ITESO - Big Data - 2024")

# Mostrar los nombres de los miembros del equipo
team_members = ["Dion Rizo", "Fernando Franco", "Daniel Rios"]
print("Team Members:")
for member in team_members:
    print(f"- {member}")

# Detener la sesión de Spark
spark.stop()
