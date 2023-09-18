import pandas as pd

from models.const import QUERY_REQ_COLS, AVAILABLE_INSTANCES_COLS
from models.scripts.m4 import calc_time_m4
from models.utils import calc_cost


# ILP Utils

def prepare_tests_from_snowflake(batch_size):
    snowflake_queries = pd.read_csv("./input/snowflake_queries.csv")
    tests = []
    for seed in range(7):
        queries = snowflake_queries.sample(n=batch_size, random_state=seed)
        queries_dict = queries.to_dict("records")
        tests.append({
            "test_id": "Test Snowflake",
            "queries": queries_dict,
            "max_instances": batch_size,
            "max_queries_per_instance": batch_size,
            "output_file": f"test_snowflake_{batch_size}_{seed}.json"
        })

    return tests


def calc_query_requests(queries, inst):
    query_requirements = []
    for q in queries:
        times = calc_time_m4(inst, q)
        best = calc_cost(times)[0].iloc[0]
        best["used_mem"] = best['used_mem_caching'] + best['used_mem_spooling']
        best["used_sto"] = best['used_sto_caching'] + best['used_sto_spooling']
        best["query_id"] = q["query_id"]
        query_requirements.append(best[QUERY_REQ_COLS])

    return query_requirements


def calc_available_instances(instances_info, max_instances):
    instances_info['mem'] = instances_info['calc_mem_caching'] + instances_info['calc_mem_spooling']
    instances_info['sto'] = instances_info['calc_sto_caching'] + instances_info['calc_sto_spooling']
    instances_info['cost_usdps'] = round(instances_info['cost_usdph'] / 3600, 6)
    instances_info = instances_info.rename(columns={
        'calc_cpu_real': 'cores',
        'calc_mem_speed': 'mem_bandwidth',
        'calc_sto_speed': 'sto_bandwidth',
        'calc_s3_speed': 's3_bandwidth'
    })[AVAILABLE_INSTANCES_COLS]

    available_instances = instances_info
    for i in range(1, max_instances):
        available_instances = pd.concat([available_instances, instances_info], axis=0, ignore_index=True)

    return available_instances
