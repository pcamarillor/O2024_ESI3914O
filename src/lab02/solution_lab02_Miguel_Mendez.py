def analyze_log(log_rdd):
    requests_per_ip = log_rdd.map(lambda x: (x.split(' ')[0], 1)).reduceByKey(lambda x, y: x + y)    
    return dict(requests_per_ip.collect())
