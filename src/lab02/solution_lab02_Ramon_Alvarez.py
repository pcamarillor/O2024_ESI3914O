def analyze_log(log_rdd):

    ips = log_rdd.map(lambda line: line.split(' ')[0]) #Paso1
    valid_ips = ips.filter(lambda ip: ip.strip() != "")
    ip_pairs = valid_ips.map(lambda ip: (ip, 1))
    ip_counts = ip_pairs.reduceByKey(lambda a, b: a + b)
    return ip_counts.collectAsMap() 