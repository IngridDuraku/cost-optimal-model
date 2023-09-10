import random
import math
import pandas as pd
from models.scripts.batch_scheduling import Scheduler
from models.utils import calc_cost

def prepare_tests():
    snowflake_queries = pd.read_csv("./input/snowflake_queries.csv")
    seed = 0
    queries = snowflake_queries.sample(n=10, random_state=seed)
    queries_dict = queries.to_dict("records")
    tests = [
        {
            "test_id": "Test Snowflake",
            "queries": queries_dict,
            "seed": seed,
            "t0": 6000,
            "iterations": 500,
            "output_file": "test_snowflake.json"
        }
    ]

    return tests

def simulate_annealing(instances, queries, kmax, t0, seed):
    random.seed(a=seed)
    scheduler = Scheduler(instances)

    for query in queries:
        results = scheduler.calc_time(query)
        best = calc_cost([results])[0].iloc[0]
        query["used_mem"] = best['used_mem_caching'] + best['used_mem_spooling']
        query["used_sto"] = best['used_sto_caching'] + best['used_sto_spooling']
        query["used_cores"] = best["used_cores"]
        query["time_cpu"] = best["time_cpu"]
        query["rw_mem"] = best["rw_mem"]
        query["rw_sto"] = best["rw_sto"]
        query["rw_s3"] = best["rw_s3"]

    current_cost = setup_initial_state(queries, scheduler)

    for k in range(0, kmax):
        t = temperature(k, t0)
        query, provision, id = neighbour(queries, scheduler)
        new_cost = neighbour_cost(current_cost, query, provision, id, scheduler)
        if acceptance_probability(current_cost, new_cost, t) >= random.random():
            apply_change(query, provision, id, scheduler)
            current_cost = new_cost
    current_state(queries)
    correctness_check(queries, current_cost)
    return current_cost


def setup_initial_state(queries, scheduler):
    total_cost = 0
    for query in queries:
        index = random.randrange(len(query["model_results"].index))
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

def neighbour(queries, scheduler):
    query = random.choice(queries)
    provisioned_instances = scheduler.suitable_provisioned_instances(query)
    instance_types = scheduler.suitable_instance_types(query)
    index = random.randrange(len(provisioned_instances.index) + 1)
    if index < len(provisioned_instances.index):
        return query, False, provisioned_instances.index[index]
    else:
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
    old_tpe_id = query["type_id"]
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

def current_state(queries):
    for query in queries:
        print("instance type: " + query["type_id"])
        print("instance id: " + query["instance_id"] + "\n")

def correctness_check(queries, current_cost):
    # TODO
    return True 
        