def analyze_log(log_rdd):
    og_rdd = log_rdd.filter(lambda line: line.strip() != "")
    ip_rdd = og_rdd.map(lambda line: line.split(' ')[0])
    ip_count_rdd = ip_rdd.map(lambda ip: (ip, 1))
    ip_counts = ip_count_rdd.reduceByKey(lambda a, b: a + b)
    result = ip_counts.collectAsMap()
    return result
