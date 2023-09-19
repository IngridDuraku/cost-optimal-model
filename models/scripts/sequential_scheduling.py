import pandas as pd
import numpy as np
import heapq

from models.scripts.m4 import calc_time_for_config_m4
from models.utils import distr_maker, model_distr_split_fn

class Scheduler:

    def __init__(self, instances) -> None:
        instances['busy_cores'] = 0
        instances['used_mem_caching'] = 0
        instances['used_sto_caching'] = 0
        instances['used_mem_spooling'] = 0
        instances['used_sto_spooling'] = 0
        self.instance_types = instances
        columns = instances.columns.to_numpy()
        # query end events is heap of tuples
        # tuples contain query finish time and used resources
        # cpu cores
        # mem for caching
        # sto for caching
        # mem for spooling
        # sto for spooling
        columns = np.append(columns, ["query_end_events"])
        self.provisioned_instances = pd.DataFrame(columns=columns)
        self.current_time = 0
        pass

    def calc_time(self, query):
        if query["arrival_time"] < self.current_time:
            raise ValueError("no time traveling queries, please.")
        self.current_time = query["arrival_time"]

        suitable_instance_types = suitable_instances(self.instance_types, query)

        results_new_instances = calc_time(suitable_instance_types, query)

        self.provisioned_instances = self.provisioned_instances.apply(lambda row: self.update_used_resources(row), axis=1)

        suitable_provisioned_instances = suitable_instances(self.provisioned_instances, query)

        if suitable_provisioned_instances.shape[0] > 0:

            results_provisioned_instances = calc_time(suitable_provisioned_instances, query)

            return pd.concat([results_new_instances, results_provisioned_instances], keys=["new_instances", "provisioned_instances"])
        
        result = pd.concat([results_new_instances], keys=["new_instances"])

        result.index.names = ["provisioned", "internal_id"]

        return result

    def update_used_resources(self, row):
        heap = row["query_end_events"] 
        while len(heap) > 0:
            head = heap[0]
            if head[0] <= self.current_time:
                row["busy_cores"] -= head[1]
                row["used_mem_caching"] -= head[2]
                row["used_sto_caching"] -= head[3]
                row["used_mem_spooling"] -= head[4]
                row["used_sto_spooling"] -= head[5]
                heapq.heappop(heap)
            else:
                break
        return row
    
    def schedule(self, query, best):
        runtime = best["stat_time_sum"]
        id = best.name[1]
        query_end_event = (
            self.current_time + runtime,
            query["per_server_cores"],
            best["used_mem_caching"],
            best["used_sto_caching"],
            best["used_mem_spooling"],
            best["used_sto_spooling"]
        )
        if best.name[0] == "new_instances":
            instance = self.instance_types.loc[id].copy()

            instance["busy_cores"] = query_end_event[1]
            instance["used_mem_caching"] = query_end_event[2]
            instance["used_sto_caching"] = query_end_event[3]
            instance["used_mem_spooling"] = query_end_event[4]
            instance["used_sto_spooling"] = query_end_event[5]

            instance["query_end_events"] = [query_end_event]

            self.provisioned_instances.loc[len(self.provisioned_instances.index)] = instance

        elif best.name[0] == "provisioned_instances":

            self.provisioned_instances.loc[id, "busy_cores"] += query_end_event[1]
            self.provisioned_instances.loc[id, "used_mem_caching"] += query_end_event[2]
            self.provisioned_instances.loc[id, "used_sto_caching"] += query_end_event[3]
            self.provisioned_instances.loc[id, "used_mem_spooling"] += query_end_event[4]
            self.provisioned_instances.loc[id, "used_sto_spooling"] += query_end_event[5]

            query_end_events = self.provisioned_instances.loc[id]["query_end_events"]
            heapq.heappush(query_end_events, (query_end_event))

def suitable_instances(instances, query):
    number_cores = instances["calc_cpu_real"]
    number_busy_cores = instances["busy_cores"]
    number_cores_needed = number_busy_cores + query["per_server_cores"]
    return instances.loc[number_cores >= number_cores_needed]

def calc_time(instances, query):
    distr_caching_precomputed = distr_maker(shape=query["cache_skew"], size=query["total_reads"])

    distr_cache = model_distr_split_fn(distr_caching_precomputed, query["first_read_from_s3"])

    spooling_read_sum = round(query["total_reads"] * query["spooling_fraction"])
    spooling_distr = [] if spooling_read_sum < 1 else distr_maker(shape=query["spooling_skew"], size=spooling_read_sum)

    scaling = 1

    return calc_time_for_config_m4(instances, query, 1, distr_cache, spooling_distr, scaling)