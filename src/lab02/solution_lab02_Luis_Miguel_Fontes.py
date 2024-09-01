def analyze_log(log_rdd):
    # Split each log line into parts
    ips, _ = log_rdd.map(lambda line: line.split(' - - '))

    # Map each IP to a tuple for counting occurrences
    ips_count = ips.map(lambda ips: (ips, 1)).reduceByKey(lambda a, b: a + b)

    # Collect and return the results into a list of tuples containing an IP and its count.
    return ips_count.collect()
