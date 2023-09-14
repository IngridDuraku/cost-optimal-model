import pandas as pd
import numpy as np

from models.scripts.m4 import calc_time_for_config_m4
from models.utils import distr_maker, model_distr_split_fn

class Scheduler:

    def __init__(self, instances) -> None:
        instances['busy_cores'] = 0
        instances['used_mem'] = 0
        instances['used_sto'] = 0
        instances['rw_mem'] = 0
        instances['rw_sto'] = 0
        instances['rw_s3'] = 0
        instances.set_index("id", inplace=True)
        self.instance_types = instances
        columns = instances.columns.to_numpy()
        columns = np.append(columns, ['query_cpu_times', 'id'])
        self.provisioned_instances = pd.DataFrame(columns=columns)
        self.provisioned_instance_id = 0
        pass

    def calc_time(self, query):
        return calc_time(self.instance_types, query)

    def schedule_new_instance(self, type_id):
        instance = self.instance_types.loc[type_id].copy()
        instance['query_cpu_times'] = []
        instance['id'] = type_id
        self.provisioned_instances.loc[str(self.provisioned_instance_id)] = instance
        self.provisioned_instance_id += 1
        return str(self.provisioned_instance_id - 1)

    def unschedule_instance(self, id):
        self.provisioned_instances.drop([id], inplace=True)

    def schedule_query_cost_new_instance(self, type_id, query):
        runtime = query["cpu_time"]
        runtime += query["rw_mem"] / self.instance_types.loc[type_id, "calc_mem_speed"]
        runtime += query["rw_sto"] / self.instance_types.loc[type_id, "calc_sto_speed"]
        runtime += query["rw_s3"] / self.instance_types.loc[type_id, "calc_s3_speed"]
        return self.instance_types.loc[type_id, "cost_usdph"] * runtime / 3600

    def schedule_query_cost_existing_instance(self, id, query):
        instance = self.provisioned_instances.loc[id]
        type_id = instance["id"]
        cpu_times = instance["query_cpu_times"]

        prev_max_cpu_time = max(cpu_times)
        new_max_cpu_time = max(prev_max_cpu_time, query["cpu_time"])

        new_runtime = new_max_cpu_time
        new_runtime += (query["rw_mem"] + instance["rw_mem"]) / self.instance_types.loc[type_id, "calc_mem_speed"]
        new_runtime += (query["rw_sto"] + instance["rw_sto"]) / self.instance_types.loc[type_id, "calc_sto_speed"]
        new_runtime += (query["rw_s3"] + instance["rw_s3"]) / self.instance_types.loc[type_id, "calc_s3_speed"]

        old_runtime = prev_max_cpu_time
        old_runtime += instance["rw_mem"] / self.instance_types.loc[type_id, "calc_mem_speed"]
        old_runtime += instance["rw_sto"] / self.instance_types.loc[type_id, "calc_sto_speed"]
        old_runtime += instance["rw_s3"] / self.instance_types.loc[type_id, "calc_s3_speed"]

        return (new_runtime - old_runtime) * instance["cost_usdph"] / 3600

    def unschedule_query_cost(self, id, query):
        instance = self.provisioned_instances.loc[id]
        type_id = instance["id"]
        cpu_times = instance["query_cpu_times"]

        prev_max_cpu_time = max(cpu_times)
        cpu_times.remove(query["cpu_time"])
        new_max_cpu_time = 0
        if len(cpu_times) > 0:
            new_max_cpu_time = max(cpu_times)
        cpu_times.append(query["cpu_time"])

        new_runtime = new_max_cpu_time
        new_runtime += (instance["rw_mem"] - query["rw_mem"]) / self.instance_types.loc[type_id, "calc_mem_speed"]
        new_runtime += (instance["rw_sto"] - query["rw_sto"]) / self.instance_types.loc[type_id, "calc_sto_speed"]
        new_runtime += (instance["rw_s3"] - query["rw_s3"]) / self.instance_types.loc[type_id, "calc_s3_speed"]

        old_runtime = prev_max_cpu_time
        old_runtime += instance["rw_mem"] / self.instance_types.loc[type_id, "calc_mem_speed"]
        old_runtime += instance["rw_sto"] / self.instance_types.loc[type_id, "calc_sto_speed"]
        old_runtime += instance["rw_s3"] / self.instance_types.loc[type_id, "calc_s3_speed"]

        return (new_runtime - old_runtime) * instance["cost_usdph"] / 3600
    
    def runtime(self, id):
        instance = self.provisioned_instances.loc[id]
        type_id = instance["id"]
        cpu_times = instance["query_cpu_times"]

        max_cpu_time = max(cpu_times)

        runtime = max_cpu_time
        runtime += instance["rw_mem"] / self.instance_types.loc[type_id, "calc_mem_speed"]
        runtime += instance["rw_sto"] / self.instance_types.loc[type_id, "calc_sto_speed"]
        runtime += instance["rw_s3"] / self.instance_types.loc[type_id, "calc_s3_speed"]

        return runtime

    def cost(self, id):
        instance = self.provisioned_instances.loc[id]
        type_id = instance["id"]
        cpu_times = instance["query_cpu_times"]

        max_cpu_time = max(cpu_times)

        runtime = max_cpu_time
        runtime += instance["rw_mem"] / self.instance_types.loc[type_id, "calc_mem_speed"]
        runtime += instance["rw_sto"] / self.instance_types.loc[type_id, "calc_sto_speed"]
        runtime += instance["rw_s3"] / self.instance_types.loc[type_id, "calc_s3_speed"]

        return runtime * instance["cost_usdph"] / 3600

    def schedule_query(self, query, id):
        self.provisioned_instances.loc[id, "busy_cores"] += query["used_cores"]
        self.provisioned_instances.loc[id, 'used_mem'] += query["used_mem"]
        self.provisioned_instances.loc[id, 'used_sto'] += query["used_sto"]
        self.provisioned_instances.loc[id, 'rw_mem'] += query["rw_mem"]
        self.provisioned_instances.loc[id, 'rw_sto'] += query["rw_sto"]
        self.provisioned_instances.loc[id, 'rw_s3'] += query["rw_s3"]

        cpu_times = self.provisioned_instances.loc[id]["query_cpu_times"]

        cpu_times.append(query["cpu_time"])

    def unschedule_query(self, query, id):
        self.provisioned_instances.loc[id, "busy_cores"] -= query["used_cores"]
        self.provisioned_instances.loc[id, 'used_mem'] -= query["used_mem"]
        self.provisioned_instances.loc[id, 'used_sto'] -= query["used_sto"]
        self.provisioned_instances.loc[id, 'rw_mem'] -= query["rw_mem"]
        self.provisioned_instances.loc[id, 'rw_sto'] -= query["rw_sto"]
        self.provisioned_instances.loc[id, 'rw_s3'] -= query["rw_s3"]

        cpu_times = self.provisioned_instances.loc[id]["query_cpu_times"]
        cpu_times.remove(query["cpu_time"])

        if len(cpu_times) == 0:
            self.unschedule_instance(id)

    def suitable_provisioned_instances(self, query):
        return suitable_instances(self.provisioned_instances, query)
    
    def suitable_instance_types(self, query):
        return suitable_instances(self.instance_types, query)

def suitable_instances(instances, query):
    number_cores = instances["calc_cpu_real"]
    number_busy_cores = instances["busy_cores"]
    number_cores_needed = number_busy_cores + query["used_cores"]
    cores_satisfied = number_cores >= number_cores_needed

    mem = instances["calc_mem_caching"] + instances["calc_mem_spooling"]
    used_mem = instances["used_mem"]
    mem_needed = used_mem + query["used_mem"]
    mem_satisfied = mem >= mem_needed

    sto = instances["calc_sto_caching"] + instances["calc_sto_spooling"]
    used_sto = instances["used_sto"]
    sto_needed = used_sto + query["used_sto"]
    sto_satisfied = sto >= sto_needed

    return instances.loc[cores_satisfied & mem_satisfied & sto_satisfied]
    
def calc_time(instances, query):
    distr_caching_precomputed = distr_maker(shape=query["cache_skew"], size=query["total_reads"])
    distr_cache = model_distr_split_fn(distr_caching_precomputed, query["first_read_from_s3"])
    spooling_read_sum = round(query["total_reads"] * query["spooling_fraction"])
    spooling_distr = [] if spooling_read_sum < 1 else distr_maker(shape=query["spooling_skew"], size=spooling_read_sum)

    scaling = 1

    return calc_time_for_config_m4(instances, 1, distr_cache, spooling_distr, scaling, query)