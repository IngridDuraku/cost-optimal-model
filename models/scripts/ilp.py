import mip

from models.const import ModelType


def run_ilp_model(query_req, available_instances, max_queries_per_instance, max_instances, model_type=ModelType.ILP):
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
            if model_type == ModelType.ILP:
                model += bits[i * query_count + q] * query_req[q]["time_cpu"] + (
                        query_req[q]["rw_mem"] / available_instances.iloc[i]["mem_bandwidth"] +
                        query_req[q]["rw_sto"] / available_instances.iloc[i]["sto_bandwidth"] +
                        query_req[q]["rw_s3"] / available_instances.iloc[i]["s3_bandwidth"]
                ) * mip.xsum(aux_bits[i * query_count * query_count + q * query_count + p] for p in range(query_count)) <= \
                         instance_runtimes[i]
            else:
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

    res = model.optimize(max_seconds=200)

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



