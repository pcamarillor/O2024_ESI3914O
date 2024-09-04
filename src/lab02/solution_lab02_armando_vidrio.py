import re

def analyze_log(log_rdd):
    # RE to find ips
    ip_pattern = re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}')

    # We have our list of ips
    raws_ips_list = log_rdd.map(lambda x: ip_pattern.findall(x))

    # We take our ips and clean invalids ips
    ips_rdd = raws_ips_list.flatMap(lambda x: x if x is not None else x)

    # We count how many times a ip is called
    ips_count_rdd = ips_rdd.countByValue()

    return ips_count_rdd
