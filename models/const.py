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
        'query_id': 1,
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
        'query_id': 2,
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

Q3 = {
        'query_id': 3,
        'cpu_h': 30,
        'total_reads': 3,
        'cache_skew': 0.1,
        'first_read_from_s3': False,
        'spooling_fraction': 0.1,
        'spooling_skew': 0.1,
        'scaling_param': 0.2,
        'max_instance_count': 1,
        "max_cores": 2
}

Q3['spooling_read_sum'] = Q3['spooling_fraction'] * Q3['total_reads']

Q4 = {
        'query_id': 4,
        'cpu_h': 10,
        'total_reads': 300,
        'cache_skew': 0.1,
        'first_read_from_s3': False,
        'spooling_fraction': 0.1,
        'spooling_skew': 0.1,
        'scaling_param': 0.2,
        'max_instance_count': 1,
        "max_cores": 2
}

Q4['spooling_read_sum'] = Q4['spooling_fraction'] * Q4['total_reads']


TESTS = [
        {
                "test_id": "Test 0",
                "queries": [Q1],
                "max_instances": 1,
                "max_queries_per_instance": 1,
                "output_file": "test_0.json"
        },
        {
                "test_id": "Test 1",
                "queries": [Q1, Q2],
                "max_instances": 2,
                "max_queries_per_instance": 2,
                "output_file": "test_1.json"
        },
        {
                "test_id": "Test 2",
                "queries": [Q1, Q2, Q3],
                "max_instances": 3,
                "max_queries_per_instance": 3,
                "output_file": "test_2.json"
        },
        {
                "test_id": "Test 3",
                "queries": [Q1, Q2, Q3, Q4],
                "max_instances": 4,
                "max_queries_per_instance": 4,
                "output_file": "test_3.json"
        },
        {
                "test_id": "Test 4",
                "queries": [Q1, Q2, Q3, Q4],
                "max_instances": 1,
                "max_queries_per_instance": 4,
                "output_file": "test_4.json"
        },
]

QUERY_REQ_COLS = [
        'query_id',
        "time_cpu",
        "rw_mem",
        "rw_sto",
        "rw_s3",
        "used_mem",
        "used_sto",
        "used_cores",
        "id",
        "cost_usdph",
        "stat_time_sum",
        "stat_price_sum"
]

AVAILABLE_INSTANCES_COLS = [
        'id',
        'mem',
        'sto',
        'cores',
        'mem_bandwidth',
        'sto_bandwidth',
        's3_bandwidth',
        'cost_usdps'
]
