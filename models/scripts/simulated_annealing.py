import random
import math
import pandas as pd
import json
from models.scripts.batch_scheduling import Scheduler
from models.utils import calc_cost

def prepare_tests():
    snowflake_queries = pd.read_csv("./input/snowflake_queries.csv")
    tests = []
    for t0 in range(100, 1000, 100):
        for p in range(1, 10, 1):
            for iterations in range(100, 1000, 100):
                seed = 10 * t0 + iterations + p
                queries = snowflake_queries.sample(n=10, random_state=seed)
                queries_dict = queries.to_dict("records")
                provision_prob = p / 10
                tests.append({
                    "test_id": "Test Snowflake",
                    "queries": queries_dict,
                    "seed": seed,
                    "t0": t0,
                    "iterations": iterations,
                    "max_per_instance": 10,
                    "provision_prob": provision_prob,
                    "output_file": "output/test_snowflake_small_t0.json"
                })
    return tests

def simulate_annealing(instances, queries, kmax, t0, seed, max_per_instance, provision_prob):
    random.seed(a=seed)
    scheduler = Scheduler(instances)

    for query in queries:
        query["total_reads"] = round(query["total_reads"])
        results = scheduler.calc_time(query)
        best = calc_cost([results])[0].iloc[0]
        query["used_mem"] = best['used_mem_caching'] + best['used_mem_spooling']
        query["used_sto"] = best['used_sto_caching'] + best['used_sto_spooling']
        query["used_cores"] = best["used_cores"]
        query["cpu_time"] = best["time_cpu"]
        query["rw_mem"] = best["rw_mem"]
        query["rw_sto"] = best["rw_sto"]
        query["rw_s3"] = best["rw_s3"]
        query["individual_cost"] = best["stat_price_sum"]
        query["individual_runtime"] = best["stat_time_sum"]
        query["individual_best_instance"] = best.name

    current_cost = setup_initial_state(queries, scheduler)
    print("initial cost: " + str(current_cost))
    for k in range(0, kmax):
        t = temperature(k, t0)
        query, provision, id = neighbour(queries, scheduler, max_per_instance, provision_prob)
        new_cost = neighbour_cost(current_cost, query, provision, id, scheduler)
        if acceptance_probability(current_cost, new_cost, t) >= random.random():
            apply_change(query, provision, id, scheduler)
            current_cost = new_cost
    result = current_state(queries, scheduler)
    #print(json.dumps(result,
    #             sort_keys=True, indent=4))
    correctness_check(result["total_cost"], current_cost)
    return result["total_cost"], result["total_cost_separated"]


def setup_initial_state(queries, scheduler):
    total_cost = 0
    for query in queries:
        index = random.randrange(len(scheduler.suitable_instance_types(query).index))
        type_id = scheduler.instance_types.index[index]
        instance_id = scheduler.schedule_new_instance(type_id)
        query["instance_id"] = instance_id
        query["type_id"] = type_id

        query_cost = scheduler.schedule_query_cost_new_instance(type_id, query)
        scheduler.schedule_query(query, instance_id)

        total_cost += query_cost
    return total_cost

def temperature(k, t0):
    return t0 / (1 + k)

def neighbour(queries, scheduler, max_per_instance, provision_prob):
    query = random.choice(queries)
    provisioned_instances = scheduler.suitable_provisioned_instances(query)
    if random.random() < provision_prob and not len(provisioned_instances.index) == 0:
        index = random.randrange(len(provisioned_instances.index))
        id = provisioned_instances.index[index]
        while max_per_instance > 0 and len(provisioned_instances.loc[id, "query_cpu_times"]) + 1 > max_per_instance:
            index = random.randrange(len(provisioned_instances.index))
            id = provisioned_instances.index[index]
        return query, False, id
    else:
        instance_types = scheduler.suitable_instance_types(query)
        index = random.randrange(len(instance_types.index))
        return query, True, instance_types.index[index]

def neighbour_cost(cost, query, provision, id, scheduler):
    if provision:
        cost += scheduler.schedule_query_cost_new_instance(id, query)
        cost += scheduler.unschedule_query_cost(query["instance_id"], query)
    else:
        if id == query["instance_id"]:
            return cost
        cost += scheduler.schedule_query_cost_existing_instance(id, query)
        cost += scheduler.unschedule_query_cost(query["instance_id"], query)
    return cost

def acceptance_probability(cost, new_cost, t):
    if new_cost < cost:
        return 1
    return math.exp(-(new_cost - cost)/t)

def apply_change(query, provision, id, scheduler):
    old_instance_id = query["instance_id"]
    if provision:
        instance_id = scheduler.schedule_new_instance(id)
        scheduler.schedule_query(query, instance_id)
        query["instance_id"] = instance_id
        query["type_id"] = id
    else:
        type_id = scheduler.provisioned_instances.loc[id, "id"]
        scheduler.schedule_query(query, id)
        query["instance_id"] = id
        query["type_id"] = type_id
    scheduler.unschedule_query(query, old_instance_id)

def current_state(queries, scheduler):
 
    total_instances = 0
    instances_summary = {}
    total_cost = 0
    total_cost_separated = 0

    for query in queries:
        instance_id = query["instance_id"]
        if instance_id not in instances_summary:
            instances_summary[instance_id] = {
                "runtime": 0,
                "type": query["type_id"],
                "cost": 0,
                "queries": []
            }
            total_instances += 1
        query_summary = {
            "id": query["query_id"],
            "individual_runtime": query["individual_runtime"],
            "individual_cost": query["individual_cost"],
            "individual_best_instance": query["individual_best_instance"]
        }
        total_cost_separated += query["individual_cost"]
        instances_summary[instance_id]["queries"].append(query_summary)

    for id, instance in instances_summary.items():
        instance["runtime"] = scheduler.runtime(id)
        instance["cost"] = scheduler.cost(id)
        total_cost += instance["cost"]


    return {
        "instance_count": total_instances,
        "execution_details": instances_summary,
        "total_cost": total_cost,
        "total_cost_separated": total_cost_separated
    }

def correctness_check(total_cost, current_cost):
    if round(total_cost, 6) != round(current_cost, 6):
        print("current_cost not equal total_cost: " + str(current_cost) + " " + str(total_cost))
        