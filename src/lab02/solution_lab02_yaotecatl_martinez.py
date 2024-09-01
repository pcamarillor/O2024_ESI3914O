def analyze_log(log_rdd):
    ip_rdd = log_rdd.filter(lambda line: line.strip() != "").map(lambda line: line.split(" ")[0])
    
    ip_counts = ip_rdd.map(lambda ip: (ip, 1)).reduceByKey(lambda a, b: a + b)
    
    ip_counts_dict = dict(ip_counts.collect())
      
    return ip_counts_dict

