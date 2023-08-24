import random
import math
from models.scripts.batch_scheduling import Scheduler
from models.utils import calc_cost

def simulate_annealing(instances, queries, kmax, t0, seed):
    random.seed(a=seed)
    scheduler = Scheduler(instances)

    for query in queries:
        results = scheduler.calc_time(query)
        results = calc_cost([results])
        query["model_results"] = results[0]

    current_cost = setup_initial_state(queries, scheduler)
    benchmark = benchmark_cost(queries)

    for k in range(0, kmax):
        t = temperature(k, t0)
        query, provision, id = neighbour(queries, scheduler)
        new_cost = neighbour_cost(current_cost, query, provision, id, scheduler)
        if acceptance_probability(current_cost, new_cost, t) >= random.random():
            apply_change(query, provision, id, scheduler)
            current_cost = new_cost
    #current_state(queries)
    #correctness_check(queries, current_cost)
    return current_cost, benchmark


def setup_initial_state(queries, scheduler):
    total_cost = 0
    for query in queries:
        index = random.randrange(len(query["model_results"].index))
        type_id = query["model_results"].index[index]
        instance_id = scheduler.schedule_new_instance(type_id)
        query["instance_id"] = instance_id
        query["type_id"] = type_id

        runtime = query["model_results"].loc[type_id, "stat_time_sum"]
        query_cost = scheduler.schedule_query_cost_new_instance(type_id, runtime)
        scheduler.schedule_query(query, instance_id, runtime)

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
        new_runtime = query["model_results"].loc[id, "stat_time_sum"]
        cost += scheduler.schedule_query_cost_new_instance(id, new_runtime)
        old_runtime = query["model_results"].loc[query["type_id"], "stat_time_sum"]
        cost += scheduler.unschedule_query_cost(query["instance_id"], old_runtime)
    else:
        if id == query["instance_id"]:
            return cost
        new_type_id = scheduler.provisioned_instances.loc[id, "id"]
        new_runtime = query["model_results"].loc[new_type_id, "stat_time_sum"]
        cost += scheduler.schedule_query_cost_existing_instance(id, new_runtime)
        old_runtime = query["model_results"].loc[query["type_id"], "stat_time_sum"]
        cost += scheduler.unschedule_query_cost(query["instance_id"], old_runtime)
    return cost

def acceptance_probability(cost, new_cost, t):
    if new_cost < cost:
        return 1
    return math.exp(-(new_cost - cost)/t)

def apply_change(query, provision, id, scheduler):
    old_instance_id = query["instance_id"]
    old_tpe_id = query["type_id"]
    if provision:
        runtime = query["model_results"].loc[id, "stat_time_sum"]
        instance_id = scheduler.schedule_new_instance(id)
        scheduler.schedule_query(query, instance_id, runtime)
        query["instance_id"] = instance_id
        query["type_id"] = id
    else:
        type_id = scheduler.provisioned_instances.loc[id, "id"]
        runtime = query["model_results"].loc[type_id, "stat_time_sum"]
        scheduler.schedule_query(query, id, runtime)
        query["instance_id"] = id
        query["type_id"] = type_id
    old_runtime = query["model_results"].loc[old_tpe_id, "stat_time_sum"]
    scheduler.unschedule_query(query, old_instance_id, old_runtime)

def current_state(queries):
    for query in queries:
        print("instance type: " + query["type_id"])
        print("instance id: " + query["instance_id"] + "\n")

def correctness_check(queries, current_cost):
    correctness_check_cost = 0
    instances = {}
    for query in queries:
        type_id = query["type_id"]
        instance_id = query["instance_id"]
        if not instance_id in instances:
            instances[instance_id] = {
                "times": [],
                "type_id": type_id,
                "cost_usdph": query["model_results"].loc[type_id, "cost_usdph"]
            }
        instances[instance_id]["times"].append(query["model_results"].loc[type_id, "stat_time_sum"])
    for instance_id in instances:
        instance = instances[instance_id]
        correctness_check_cost += instance["cost_usdph"] * max(instance["times"]) / 3600
    if round(current_cost) != round(correctness_check_cost):
        return False
    return True 

def benchmark_cost(queries):
    individually_optimal_cost = 0
    for query in queries:
        individually_optimal_cost += query["model_results"].iloc[0]["stat_price_sum"]
    return individually_optimal_cost 
        