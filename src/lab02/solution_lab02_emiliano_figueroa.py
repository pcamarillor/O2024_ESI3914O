def analyze_log(log_rdd):
    # Filtra las líneas que tienen al menos una IP válida y evita líneas vacías
    filtered_rdd = log_rdd.filter(lambda line: len(line.split()) > 0 and line.split()[0].count('.') == 3)

    # Mapa de extracción para cada IP. Se asume que la IP está en la primera posición de la línea
    ip_rdd = filtered_rdd.map(lambda line: (line.split()[0], 1))

    # Agrupa por IP y cuenta las ocurrencias
    ip_count_rdd = ip_rdd.reduceByKey(lambda a, b: a + b)

    # Convierte el RDD a diccionario
    ip_count = ip_count_rdd.collectAsMap()

    return ip_count
