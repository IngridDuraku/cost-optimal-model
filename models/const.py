# base workload
CPU_H = 20
TOTAL_READS = 800

# caching
CACHE_SKEW = 0.1
FIRST_READ_FROM_S3 = False

# materialization
SPOOLING_FRACTION = 0.2
SPOOLING_SKEW = 0.1
SPOOLING_READ_SUM = TOTAL_READS * SPOOLING_FRACTION

# scaling
SCALING_PARAM = 0.98  # - portion of the workload that can be parallelized across instances
MAX_INSTANCE_COUNT = 32


DEFAULT_PARAMS = {
    'cpu_h': 20,
    'total_reads': 20,
    'cache_skew': 0.1,
    'first_read_from_s3': False,
    'spooling_fraction': 0.2,
    'spooling_skew': 0.1,
    'spooling_read_sum': TOTAL_READS * SPOOLING_FRACTION,
    'scaling_param': 0.98,
    'max_instance_count': 32,
    "max_cores": 3
}

Q1 = {
        'cpu_h': 2,
        'total_reads': 800,
        'cache_skew': 0.6,
        'first_read_from_s3': False,
        'spooling_fraction': 0.2,
        'spooling_skew': 0.5,
        'scaling_param': 0.1,
        'max_instance_count': 1,
        "max_cores": 3
}

Q1['spooling_read_sum'] = Q1['spooling_fraction'] * Q1['total_reads']

Q2 = {
        'cpu_h': 30,
        'total_reads': 3,
        'cache_skew': 0.4,
        'first_read_from_s3': False,
        'spooling_fraction': 0.5,
        'spooling_skew': 0.3,
        'scaling_param': 0.2,
        'max_instance_count': 1,
        "max_cores": 3
}

Q2['spooling_read_sum'] = Q2['spooling_fraction'] * Q2['total_reads']


QUERIES = [Q1, Q2]
