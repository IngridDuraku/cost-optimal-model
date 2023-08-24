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
        "cpu_h": 200,
        "total_reads": 800,
        "cache_skew": 0.1,
        "first_read_from_s3": True,
        "spooling_fraction": 0.2,
        "spooling_skew": 0.1,
        "scaling_param": 0.98,
        "per_server_cores": 8,
        "arrival_time": 0,
        "mem_request": 92,
        "sto_request": 0,
    },
    {
        "cpu_h": 100,
        "total_reads": 160,
        "cache_skew": 0.9,
        "first_read_from_s3": True,
        "spooling_fraction": 0.1,
        "spooling_skew": 0.1,
        "scaling_param": 0.95,
        "per_server_cores": 1,
        "arrival_time": 0,
        "mem_request": 92,
        "sto_request": 0,
    },
    {
        "cpu_h": 1000,
        "total_reads": 5000,
        "cache_skew": 0.1,
        "first_read_from_s3": True,
        "spooling_fraction": 0.2,
        "spooling_skew": 0.1,
        "scaling_param": 0.98,
        "per_server_cores": 36,
        "arrival_time": 0,
        "mem_request": 48,
        "sto_request": 400
    },
    {
        "cpu_h": 700,
        "total_reads": 1024,
        "cache_skew": 0.5,
        "first_read_from_s3": True,
        "spooling_fraction": 0.01,
        "spooling_skew": 0.3,
        "scaling_param": 0.9,
        "per_server_cores": 4,
        "arrival_time": 0,
        "mem_request": 92,
        "sto_request": 200,
    },
    {
        "cpu_h": 50,
        "total_reads": 5000,
        "cache_skew": 0.2,
        "first_read_from_s3": True,
        "spooling_fraction": 0.1,
        "spooling_skew": 0.2,
        "scaling_param": 0.8,
        "per_server_cores": 8,
        "arrival_time": 0,
        "mem_request": 48,
        "sto_request": 300,
    },
    {
        "cpu_h": 50,
        "total_reads": 10,
        "cache_skew": 0.2,
        "first_read_from_s3": True,
        "spooling_fraction": 0.1,
        "spooling_skew": 0.2,
        "scaling_param": 0.8,
        "per_server_cores": 8,
        "arrival_time": 0,
        "mem_request": 24,
        "sto_request": 0,
    }
]