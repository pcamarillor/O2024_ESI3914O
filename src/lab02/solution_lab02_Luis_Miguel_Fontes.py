def analyze_log(log_rdd):
    ips = log_rdd.map(lambda line: line.split(' - - ')[0]).filter(lambda ip: ip.strip() != "")
    ips_count = ips.map(lambda ip: (ip, 1)).reduceByKey(lambda a, b: a + b)

    return dict(ips_count.collect())
