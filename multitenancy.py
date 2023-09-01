from itertools import combinations

from models.const import Q1, Q2
from models.scripts.m4 import calc_time_m4
from models.utils import calc_cost
from preprocessing.instances import instSet_transform

INSTANCE_COUNT = 1
QUERIES_PER_INSTANCE = 2

QUERIES = [
    {
        'cpu_h': 20,
        'total_reads': 800,
        'cache_skew': 0.1,
        'first_read_from_s3': False,
        'spooling_fraction': 0.2,
        'spooling_skew': 0.1,
        'scaling_param': 0.98,
        'max_instance_count': 1
    },
    {
        'cpu_h': 56, # sum
        'total_reads': 300, # sum
        'cache_skew': 0.4, # mesatare e ponderuar
        'first_read_from_s3': False, # global param or don't schedule together if different
        'spooling_fraction': 0.5, # get from spooling read sum
        'spooling_skew': 0.3, # mesatare e ponderuar
        'scaling_param': 0.8, # min ?
        'max_instance_count': 1
    },
]


def combine_queries(*queries):
    total_reads = 0
    cpu_h = 0
    cache_skew = 0
    spooling_skew = 0
    spooling_read_sum = 0
    scaling_param = 0

    for query in queries:
        total_reads += query['total_reads']
        cpu_h += query['cpu_h']
        cache_skew += query['cache_skew'] * query['total_reads']
        spooling_read_sum += query['spooling_fraction'] * query['total_reads']
        spooling_skew += query['spooling_skew'] * query['spooling_fraction'] * query['total_reads']
        scaling_param += query["scaling_param"]

    return {
        "cpu_h": cpu_h,
        "total_reads": total_reads,
        "cache_skew": round(cache_skew / total_reads, 2),
        "spooling_read_sum": spooling_read_sum,
        "spooling_fraction": round(spooling_read_sum / total_reads, 2),
        "spooling_skew": round(spooling_skew / spooling_read_sum, 2),
        "max_instance_count": 1,
        "scaling_param": scaling_param/len(queries) + 0.5,
        "first_read_from_s3": False
    }


if __name__ == "__main__":
    instances = instSet_transform()
    query = combine_queries(Q1, Q2)

    print(query)
    instances = calc_time_m4(instances, params=query)
    instances = calc_cost(instances)
    index = 1
    for i in instances:
        i.to_csv("./output/output_multi_12" + str(index) + ".csv")
        index += 1
