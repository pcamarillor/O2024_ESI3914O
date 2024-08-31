def analyze_log(log_rdd):

    #filter que tenga al menos 1 palabra 
    log_rdd = log_rdd.filter(lambda line: len(line.split()) > 0 )
    #extract IP's
    extract = log_rdd.map(lambda line: line.split()[0]) 
    #count ocurrencias de cada IP
    countIp = extract.map(lambda ip: (ip, 1)).reduceByKey(lambda a, b: a + b)
    #convertir a diccionario
    result = dict(countIp.collect())
    
    return result


  