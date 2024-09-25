def analyze_log(logs_rdd):

    # Function to extract the IP address, assumes IP is the first part of the log line
    def extract_ip(log_line):
        parts = log_line.split(" ")
        return parts[0]

    # Map each log to a tuple (IP, 1)
    ip_mapped = logs_rdd.map(lambda log: (extract_ip(log), 1))

    # Filter out any None or empty IP addresses
    filtered_ip_mapped = ip_mapped.filter(lambda x: x[0] is not None and x[0] != "")

    # Reduce by key to count the occurrences per IP
    ip_counts = filtered_ip_mapped.reduceByKey(lambda a, b: a + b)

    # Convert RDD to a dictionary
    return dict(ip_counts.collect())
