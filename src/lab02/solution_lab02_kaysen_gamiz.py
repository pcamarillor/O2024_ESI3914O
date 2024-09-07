def analyze_log(log_rdd):

    def filter_empty_lines(line):
        return line.strip() != ''

    def extract_ip(line):
        return line.split(" ")[0]

    def map_ip_to_one(ip):
        return (ip, 1)

    def reduce_ip_counts(count1, count2):
        return count1 + count2

    ip_counts = (
        log_rdd
        .filter(filter_empty_lines)  # Filtering empty lines
        .map(extract_ip)  # Splitting ips in the first line
        .map(map_ip_to_one)  # key-value for ip 
        .reduceByKey(reduce_ip_counts) # Count IP ocurrences
        .collectAsMap() # To dictionary
    )

    return ip_counts
