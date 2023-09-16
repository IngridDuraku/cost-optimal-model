from models.const import QUERY_REQ_COLS, AVAILABLE_INSTANCES_COLS
from models.scripts.m4 import calc_time_m4
from models.utils import calc_cost

import mip
import pandas as pd
import json


def prepare_tests(batch_size):
    snowflake_queries = pd.read_csv("./input/snowflake_queries.csv")
    queries = snowflake_queries.sample(n=10)
    queries_dict = queries.to_dict("records")
    tests = [
        {
            "test_id": "Test Snowflake",
            "queries": queries_dict,
            "max_instances": batch_size,
            "max_queries_per_instance": batch_size,
            "output_file": "test_snowflake.json"
        }
    ]

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


def run_ilp_bw_model(query_req, available_instances, max_queries_per_instance, max_instances, output_file):
    query_count = len(query_req)
    instance_count = len(available_instances)

    # model
    model = mip.Model('cost-optimal')

    # decision variables
    bits = [model.add_var(var_type=mip.BINARY, name=f"bit{i}{q}") for i in range(instance_count) for q in
            range(query_count)]
    instance_runtimes = [model.add_var(var_type=mip.CONTINUOUS, name=f"runtime{i}") for i in range(0, instance_count)]
    aux_bits = [model.add_var(var_type=mip.BINARY) for i in range(instance_count) for q1 in range(query_count) for q2 in
                range(query_count)]
    inst_bits = [model.add_var(var_type=mip.BINARY) for i in range(0, instance_count)]

    # objective function
    model.objective = mip.minimize(
        mip.xsum(instance_runtimes[i] * available_instances.iloc[i]["cost_usdps"] for i in range(instance_count)))

    # constraints
    for i in range(instance_count):
        model += mip.xsum(bits[i * query_count + q] for q in range(query_count)) <= max_queries_per_instance

    for i in range(instance_count):
        for q in range(query_count):
            model += inst_bits[i] >= bits[i * query_count + q]

    for i in range(instance_count):
        model += inst_bits[i] <= mip.xsum(bits[i * query_count + q] for q in range(query_count))

    model += mip.xsum(inst_bits[i] for i in range(instance_count)) <= max_instances

    for q in range(query_count):
        model += mip.xsum(bits[i * query_count + q] for i in range(instance_count)) == 1

    for i in range(instance_count):
        model += mip.xsum(bits[i * query_count + q] * query_req[q]['used_cores'] for q in range(query_count)) <= \
                 available_instances.iloc[i]["cores"]

    for i in range(instance_count):
        model += mip.xsum(bits[i * query_count + q] * query_req[q]['used_mem'] for q in range(query_count)) <= \
                 available_instances.iloc[i]["mem"]

    for i in range(instance_count):
        model += mip.xsum(bits[i * query_count + q] * query_req[q]['used_sto'] for q in range(query_count)) <= \
                 available_instances.iloc[i]["sto"]

    for i in range(instance_count):
        for q in range(query_count):
            model += bits[i * query_count + q] * query_req[q]["time_cpu"] + (
                    mip.xsum(
                        query_req[p]["rw_mem"] * aux_bits[i * query_count * query_count + q * query_count + p]
                        for p in range(query_count)
                    ) / available_instances.iloc[i]["mem_bandwidth"] +
                    mip.xsum(
                        query_req[p]["rw_sto"] * aux_bits[i * query_count * query_count + q * query_count + p]
                        for p in range(query_count)
                    ) / available_instances.iloc[i]["sto_bandwidth"] +
                    mip.xsum(
                        query_req[p]["rw_s3"] * aux_bits[i * query_count * query_count + q * query_count + p]
                        for p in range(query_count)
                    ) / available_instances.iloc[i]["s3_bandwidth"]
            ) <= instance_runtimes[i]


    for i in range(instance_count):
        for q in range(query_count):
            for p in range(query_count):
                if p == q:
                    model += aux_bits[i * query_count * query_count + q * query_count + p] == bits[i * query_count + q]
                    continue
                model += aux_bits[i * query_count * query_count + q * query_count + p] <= bits[i * query_count + q]
                model += aux_bits[i * query_count * query_count + q * query_count + p] <= bits[i * query_count + p]
                model += aux_bits[i * query_count * query_count + q * query_count + p] >= bits[i * query_count + p] + \
                         bits[i * query_count + q] - 1

    res = model.optimize(max_seconds=100)


    if res.value == 5:
        return {
            "error": "Algorithm terminated. Not enough time.",
            "status": res.value,
        }

    total_instances = 0
    instances_summary = []
    total_cost = 0
    total_cost_separated = 0

    for i in range(instance_count):
        if instance_runtimes[i].x == 0:
            continue
        print(f"{available_instances.iloc[i]['id']} ({instance_runtimes[i].x})")
        local_cost = instance_runtimes[i].x * available_instances.iloc[i]["cost_usdps"]
        i_details = {
            "instance_id": available_instances.iloc[i]['id'],
            "runtime": instance_runtimes[i].x,
            "cost": local_cost,
            "sto": float(available_instances.iloc[i]['sto']),
            "mem": float(available_instances.iloc[i]['mem']),
            "cores": float(available_instances.iloc[i]['cores']),
            "mem_bandwidth": float(available_instances.iloc[i]['mem_bandwidth']),
            "sto_bandwidth": float(available_instances.iloc[i]['sto_bandwidth']),
            "s3_bandwidth": float(available_instances.iloc[i]['s3_bandwidth']),
            "queries": []
        }
        total_cost += local_cost
        total_instances += 1

        for q in range(query_count):
            if bits[i * query_count + q].x:
                q_details = {
                    "id": query_req[q]['query_id'],
                    "best_instance": query_req[q]['id'],
                    "individual_runtime": query_req[q]['stat_time_sum'],
                    "individual_cost": query_req[q]['stat_price_sum'],
                    "rw_mem": float(query_req[q]['rw_mem']),
                    "rw_sto": float(query_req[q]['rw_sto']),
                    "rw_s3": float(query_req[q]['rw_s3']),
                    "time_cpu": query_req[q]['time_cpu'],
                    "used_mem": float(query_req[q]['used_mem']),
                    "used_sto": float(query_req[q]['used_sto']),
                    "used_cores": float(query_req[q]['used_cores']),
                }
                total_cost_separated += query_req[q]['stat_price_sum']
                i_details["queries"].append(q_details)

        instances_summary.append(i_details)

    final_result = {
        "instance_count": total_instances,
        "execution_details": instances_summary,
        "total_cost": total_cost,
        "total_cost_separated": total_cost_separated,
        "status": res.value
    }

    return final_result

