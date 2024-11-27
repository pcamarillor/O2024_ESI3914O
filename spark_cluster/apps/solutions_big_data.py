from pyspark.sql import SparkSession

# Crear la SparkSession
spark = SparkSession.builder \
    .appName("UserDataToMySQL") \
    .getOrCreate()

# Configuración de la base de datos MySQL
mysql_url = "jdbc:mysql://mysql-iteso:3306/iteso_db"  # URL de la base de datos
mysql_properties = {
    "user": "iteso_user",  # Usuario definido en la configuración del contenedor
    "password": "iteso_password",  # Contraseña definida en la configuración del contenedor
    "driver": "com.mysql.cj.jdbc.Driver"  # Driver de MySQL para la conexión
}

# Leer el archivo CSV con los datos de usuarios
user_data_df = spark.read.csv(
    "/opt/spark-data/user_data2.csv",  # Ruta del archivo CSV dentro del contenedor
    header=True,  # Indica que el archivo tiene encabezados
    inferSchema=True  # Deduce automáticamente los tipos de datos
)

# Escribir los datos en la tabla de MySQL
user_data_df.write.jdbc(
    url=mysql_url,  # Conexión a la base de datos
    table="user_data",  # Nombre de la tabla en MySQL
    mode="overwrite",  # Sobrescribe la tabla si ya existe
    properties=mysql_properties  # Propiedades para la conexión
)