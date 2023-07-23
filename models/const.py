# base workload
CPU_H = 20
TOTAL_READS = 3000

# caching
CACHE_SKEW = 1.2
FIRST_READ_FROM_S3 = False

# materialization
SPOOLING_FRACTION = 0.5
SPOOLING_SKEW = 1.3
SPOOLING_READ_SUM = TOTAL_READS * SPOOLING_FRACTION

# scaling
SCALING_PARAM = 0.7  # - portion of the workload that can be parallelized
MAX_INSTANCE_COUNT = 32
