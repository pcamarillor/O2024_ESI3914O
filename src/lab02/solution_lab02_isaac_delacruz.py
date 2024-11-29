def analyze_log(log_rdd):
    ip_count = log_rdd.filter(lambda line: line != "").map(lambda line: (line.split(" ")[0], 1)).reduceByKey(lambda a, b: a + b)
    return ip_count.collectAsMap()