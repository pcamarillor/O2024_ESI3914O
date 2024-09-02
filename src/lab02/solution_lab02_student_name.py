def analyze_log(log_rdd):
    # Filter the missing lines
    filtered_rdd = log_rdd.filter(lambda line: len(line.split()) > 0)

    # Count the 
    ip_rdd = filtered_rdd.map(lambda line: (line.split()[0], 1))

    # ID groups
    ip_count_rdd = ip_rdd.reduceByKey(lambda a, b: a + b)

    # RDD to dic
    ip_count = ip_count_rdd.collectAsMap()

    return ip_count
