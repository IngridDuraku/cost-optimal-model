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
SCALING_PARAM = 0.98  # - portion of the workload that can be parallelized
MAX_INSTANCE_COUNT = 32

QUERIES = [
    {
        "cpu_h": 20,
        "total_reads": 800,
        "cache_skew": 0.1,
        "first_read_from_s3": False,
        "spooling_fraction": 0.2,
        "spooling_skew": 0.1,
        "scaling_param": 0.98,
    },
    {
        "cpu_h": 1,
        "total_reads": 16000,
        "cache_skew": 0.9,
        "first_read_from_s3": False,
        "spooling_fraction": 0.1,
        "spooling_skew": 0.1,
        "scaling_param": 0.95,
    },
]