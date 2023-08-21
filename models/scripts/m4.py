import pandas as pd

from models.const import MAX_INSTANCE_COUNT
from models.utils import model_distr_pack, distr_maker, model_distr_split_fn, model_make_scaling


def calc_time_for_config_m4(inst, query, count, distr_cache, distr_spooling, scale):
    #inst = inst.reset_index()
    bins_cache = {
        'data_mem': pd.DataFrame(
            data={'size': (inst['calc_mem_caching'] - inst['used_mem_caching']).round(decimals=0), 'prio': inst['calc_mem_speed']}),
        'data_sto': pd.DataFrame(
            data={'size': (inst['calc_sto_caching'] - inst['used_sto_caching']).round(decimals=0), 'prio': inst['calc_sto_speed']}),
        'data_s3': pd.DataFrame(
            data={'size': [len(distr_cache['working'])] * len(inst), 'prio': inst['calc_net_speed']})
    }

    bins_spooling = {
        'data_mem': pd.DataFrame(
            data={'size': (inst['calc_mem_spooling'] - inst['used_mem_spooling']).round(decimals=0), 'prio': inst['calc_mem_speed']}),
        'data_sto': pd.DataFrame(
            data={'size': (inst['calc_sto_spooling'] - inst['used_sto_spooling']).round(decimals=0), 'prio': inst['calc_sto_speed']}),
        'data_s3': pd.DataFrame(data={'size': [len(distr_spooling)] * len(inst), 'prio': inst['calc_net_speed']})
    }

    mem_read_distribution = model_distr_pack(bins_cache, distr_cache['working'])
    spool_read_distribution = model_distr_pack(bins_spooling, distr_spooling)

    spool_sum = round(sum(distr_spooling))
    inv_eff = count * scale

    result = pd.DataFrame(
        columns=[
            "id_name",
            "count",
            "id",
            "cost_usdph",
            "read_cache_load",
            "read_cache_mem",
            "used_mem_caching",
            "calc_mem_caching", # temp
            "read_cache_sto",
            "used_sto_caching",
            "calc_sto_caching", # temp
            "read_cache_s3",
            "read_spool_mem",
            "used_mem_spooling",
            "calc_mem_spooling", # temp
            "read_spool_sto",
            "used_sto_spooling",
            "calc_sto_spooling", # temp
            "read_spool_s3",
            "rw_mem",
            "rw_sto",
            "rw_s3",
            "rw_xchg",
            "stat_read_spool",
            "stat_read_work",
            "time_cpu",
            "time_mem",
            "time_sto",
            "time_s3",
            "time_xchg",
            "stat_time_sum",
            "stat_time_max",
        ]
    )

    result[["calc_mem_caching","calc_sto_caching","calc_mem_spooling","calc_sto_spooling"]] = inst[["calc_mem_caching","calc_sto_caching","calc_mem_spooling","calc_sto_spooling"]]

    result["id_name"] = inst["id"]
    result["count"] = count
    result["id"] = result["id_name"] + "/" + str(count)
    result["cost_usdph"] = (inst["cost_usdph"] * count).round(3)

    result["read_cache_load"] = sum(distr_cache["initial"]).round(2)
    result["read_cache_mem"] = mem_read_distribution['data_mem'].round(2)
    result["read_cache_sto"] = mem_read_distribution['data_sto'].round(2)
    result["read_cache_s3"] = mem_read_distribution['data_s3'].round(2)

    result["read_spool_mem"] = spool_read_distribution['data_mem'].round(2)
    result["read_spool_sto"] = spool_read_distribution['data_sto'].round(2)
    result["read_spool_s3"] = spool_read_distribution['data_s3'].round(2)

    result["used_mem_caching"] = mem_read_distribution['data_stored_mem']
    result["used_sto_caching"] = mem_read_distribution['data_stored_sto']
    result["used_mem_spooling"] = mem_read_distribution['data_stored_mem']
    result["used_sto_spooling"] = mem_read_distribution['data_stored_sto']

    result["rw_mem"] = result["read_cache_mem"] + 2 * result["read_spool_mem"].round(2)
    result["rw_sto"] = result["read_cache_sto"] + 2 * result["read_spool_sto"].round(2)
    result["rw_s3"] = result["read_cache_s3"] + 2 * result["read_spool_s3"].round(2)

    # shouldn't this be 0 at count == 1?
    result["rw_xchg"] = 0 if count == 0 else 2 * spool_sum

    result["stat_read_spool"] = spool_sum
    result["stat_read_work"] = round(sum(distr_cache["working"]))

    result["time_cpu"] = round((query["cpu_h"] * 3600 / query['per_server_cores']) * scale, 2)
    result["time_mem"] = ((result["rw_mem"] / inst["calc_mem_speed"]) * inv_eff).round(2)
    result["time_sto"] = ((result["rw_sto"] / inst["calc_sto_speed"]) * inv_eff).round(2)
    result["time_s3"] = ((result["rw_s3"] / inst["calc_s3_speed"]) * inv_eff).round(2)

    result["time_xchg"] = ((result["rw_xchg"] / 2 / inst["calc_net_speed"]) * inv_eff).round(2)
    result["time_load"] = ((result["read_cache_load"] / inst["calc_s3_speed"]) * inv_eff).round(2)
    result["stat_time_sum"] = result["time_s3"] + result["time_sto"] + result["time_mem"] + result["time_xchg"] \
                              + result["time_load"] + result["time_cpu"]
    result["stat_time_max"] = result[["time_s3", "time_sto", "time_mem", "time_xchg", "time_load", "time_cpu"]].max(
        axis=1
    )

    result["scale"] = inv_eff

    return result


def calc_time_m4(instances, query):
    distr_caching_precomputed = [
        # why not apply the scale here
        distr_maker(shape=query["cache_skew"], size=round(query["total_reads"] / n))
        for n in range(1, MAX_INSTANCE_COUNT + 1)
    ]
    distr_cache = list(map(lambda x: model_distr_split_fn(x, query["first_read_from_s3"]), distr_caching_precomputed))

    spooling_read_sum = query["total_reads"] * query["spooling_fraction"]
    spooling_distr = [
        [] if round(spooling_read_sum/n) < 1 else distr_maker(shape=query["spooling_skew"], size=round(spooling_read_sum / n))
        for n in range(1, MAX_INSTANCE_COUNT + 1)
    ]

    scaling = [model_make_scaling(query["scaling_param"], n) for n in range(1, MAX_INSTANCE_COUNT+1)]
    result = [
        calc_time_for_config_m4(instances, query, i, distr_cache[i-1], spooling_distr[i-1], scaling[i-1])
        for i in range(1, MAX_INSTANCE_COUNT+1)
    ]

    return result

