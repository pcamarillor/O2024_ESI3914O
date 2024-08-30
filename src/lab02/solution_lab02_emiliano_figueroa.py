def analyze_log(log_rdd):
    # Extraer las direcciones IP de cada línea del log
    def extract_ip(line):
        return line.split(' ')[0]

    # Paso 1: Extraer las IPs
    ips_rdd = log_rdd.map(extract_ip)

    # Paso 2: Contar el número de solicitudes por cada IP
    ip_counts_rdd = ips_rdd.map(lambda ip: (ip, 1)).reduceByKey(lambda a, b: a + b)

    # Paso 3: Convertir el resultado a un diccionario
    ip_counts = dict(ip_counts_rdd.collect())

    # Retornar el resultado como un diccionario
    return ip_counts
