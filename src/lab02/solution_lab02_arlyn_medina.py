def analyze_log(log_rdd):
    # Filtra las líneas que están vacías
    filtered_rdd = log_rdd.filter(lambda line: len(line.split()) > 0)

    # Mapa de extracción para cada ip
    ip_rdd = filtered_rdd.map(lambda line: (line.split()[0], 1))

    # Agrupa por id
    ip_count_rdd = ip_rdd.reduceByKey(lambda a, b: a + b)

    # Convierte el RDD a diccionario
    ip_count = ip_count_rdd.collectAsMap()

    return ip_count
