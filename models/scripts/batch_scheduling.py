import pandas as pd
import numpy as np

from models.scripts.m4 import calc_time_for_config_m4
from models.utils import distr_maker, model_distr_split_fn

class Scheduler:

    def __init__(self, instances) -> None:
        instances['busy_cores'] = 0
        instances['used_mem_caching'] = 0
        instances['used_sto_caching'] = 0
        instances['used_mem_spooling'] = 0
        instances['used_sto_spooling'] = 0
        instances.set_index("id", inplace=True)
        self.instance_types = instances
        columns = instances.columns.to_numpy()
        columns = np.append(columns, ['query_runtimes', 'id'])
        self.provisioned_instances = pd.DataFrame(columns=columns)
        self.provisioned_instance_id = 0
        pass

    def calc_time(self, query):

        suitable_instance_types = suitable_instances(self.instance_types, query)

        return calc_time(suitable_instance_types, query)

    def schedule_new_instance(self, type_id):
        instance = self.instance_types.loc[type_id].copy()
        instance['query_runtimes'] = []
        instance['id'] = type_id
        self.provisioned_instances.loc[str(self.provisioned_instance_id)] = instance
        self.provisioned_instance_id += 1
        return str(self.provisioned_instance_id - 1)

    def unschedule_instance(self, id):
        self.provisioned_instances.drop([id], inplace=True)

    def schedule_query_cost_new_instance(self, type_id, runtime):
        return self.instance_types.loc[type_id, "cost_usdph"] * runtime / 3600

    def schedule_query_cost_existing_instance(self, id, runtime):
        runtimes = self.provisioned_instances.loc[id, "query_runtimes"]
        prev_max = max(runtimes)

        # if the previous max runtime is smaller than the query's runtime, the cost is increased
        return max(runtime - prev_max, 0) * self.provisioned_instances.loc[id, "cost_usdph"] / 3600

    def unschedule_query_cost(self, id, runtime):
        runtimes = self.provisioned_instances.loc[id]["query_runtimes"]
        runtimes.remove(runtime)
        new_max = 0
        if len(runtimes) > 0:
            new_max = max(runtimes)
        runtimes.append(runtime)

        # if the new max runtime is smaller than the query's runtime, the cost is decreased
        return min(new_max - runtime, 0) * self.provisioned_instances.loc[id, "cost_usdph"] / 3600

    def schedule_query(self, query, id, runtime):
        self.provisioned_instances.loc[id, "busy_cores"] += query["per_server_cores"]
        self.provisioned_instances.loc[id, 'used_mem_caching'] += query["mem_request"]
        self.provisioned_instances.loc[id, 'used_sto_caching'] += query["mem_request"]
        self.provisioned_instances.loc[id, 'used_mem_spooling'] += query["sto_request"]
        self.provisioned_instances.loc[id, 'used_sto_spooling'] += query["sto_request"]

        runtimes = self.provisioned_instances.loc[id]["query_runtimes"]
        runtimes.append(runtime)

    def unschedule_query(self, query, id, runtime):
        self.provisioned_instances.loc[id, "busy_cores"] -= query["per_server_cores"]
        self.provisioned_instances.loc[id, 'used_mem_caching'] -= query["mem_request"]
        self.provisioned_instances.loc[id, 'used_sto_caching'] -= query["mem_request"]
        self.provisioned_instances.loc[id, 'used_mem_spooling'] -= query["sto_request"]
        self.provisioned_instances.loc[id, 'used_sto_spooling'] -= query["sto_request"]

        runtimes = self.provisioned_instances.loc[id]["query_runtimes"]
        runtimes.remove(runtime)

        if len(runtimes) == 0:
            self.unschedule_instance(id)

    def suitable_provisioned_instances(self, query):
        return suitable_instances(self.provisioned_instances, query)
    
    def suitable_instance_types(self, query):
        return suitable_instances(self.instance_types, query)

def suitable_instances(instances, query):
    number_cores = instances["calc_cpu_real"]
    number_busy_cores = instances["busy_cores"]
    number_cores_needed = number_busy_cores + query["per_server_cores"]
    cores_satisfied = number_cores >= number_cores_needed

    mem_caching = instances["calc_mem_caching"]
    used_mem_caching = instances["used_mem_caching"]
    mem_caching_needed = used_mem_caching + query["mem_request"]
    mem_caching_satisfied = mem_caching >= mem_caching_needed

    sto_caching = instances["calc_sto_caching"]
    used_sto_caching = instances["used_sto_caching"]
    sto_caching_needed = used_sto_caching + query["sto_request"]
    sto_caching_satisfied = sto_caching >= sto_caching_needed

    mem_spooling = instances["calc_mem_spooling"]
    used_mem_spooling = instances["used_mem_spooling"]
    mem_spooling_needed = used_mem_spooling + query["mem_request"]
    mem_spooling_satisfied = mem_spooling >= mem_spooling_needed

    sto_spooling = instances["calc_sto_spooling"]
    used_sto_spooling = instances["used_sto_spooling"]
    sto_spooling_needed = used_sto_spooling + query["sto_request"]
    sto_spooling_satisfied = sto_spooling >= sto_spooling_needed

    return instances.loc[cores_satisfied & mem_caching_satisfied & sto_caching_satisfied & mem_spooling_satisfied & sto_spooling_satisfied]
    
def calc_time(instances, query):
    distr_caching_precomputed = distr_maker(shape=query["cache_skew"], size=query["total_reads"])

    distr_cache = model_distr_split_fn(distr_caching_precomputed, query["first_read_from_s3"])

    spooling_read_sum = round(query["total_reads"] * query["spooling_fraction"])
    spooling_distr = [] if spooling_read_sum < 1 else distr_maker(shape=query["spooling_skew"], size=spooling_read_sum)

    scaling = 1

    return calc_time_for_config_m4(instances, query, 1, distr_cache, spooling_distr, scaling)