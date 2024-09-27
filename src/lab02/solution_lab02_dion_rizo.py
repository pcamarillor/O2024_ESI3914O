def analyze_log(log_rdd):
    # Primero, mapeamos cada línea del archivo de logs a una tupla (IP, 1)
    ip_counts = log_rdd.map(lambda line: (line.split()[0], 1))
    
    # Luego, reducimos las tuplas sumando los valores para cada IP
    ip_counts = ip_counts.reduceByKey(lambda a, b: a + b)
    
    # Finalmente, convertimos el resultado a un diccionario
    result = dict(ip_counts.collect())
    
    return result

    return None
