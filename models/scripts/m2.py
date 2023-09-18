import pandas as pd

from ..const import DEFAULT_PARAMS
from ..utils import model_distr_pack, distr_maker, model_distr_split_fn


def calc_time_for_config_m2(inst, distr_cache, cpu_h):
    inst = inst.reset_index()
    data_mem = pd.DataFrame(data={'size': inst['calc_mem_caching'].round(decimals=0), 'prio': inst['calc_mem_speed']})
    data_sto = pd.DataFrame(data={'size': inst['calc_sto_caching'].round(decimals=0), 'prio': inst['calc_sto_speed']})
    data_s3 = pd.DataFrame(data={'size': [len(distr_cache['working'])] * len(inst), 'prio': inst['calc_net_speed']})

    bins_cache = {
        'data_mem': data_mem,
        'data_sto': data_sto,
        'data_s3': data_s3
    }

    mem_read_distribution = model_distr_pack(bins_cache, distr_cache['working'])
    cpu_time = cpu_h / inst['vcpu_count']
    inst['data_mem'] = mem_read_distribution['data_mem']
    inst['data_sto'] = mem_read_distribution['data_sto']
    inst['data_s3'] = mem_read_distribution['data_s3']

    scan_time = mem_read_distribution['data_mem'] / inst['calc_mem_speed'] + mem_read_distribution['data_sto'] / inst['calc_sto_speed'] + mem_read_distribution['data_s3'] / inst['calc_net_speed']
    inst['execution_time_m2'] = cpu_time + scan_time

    return inst


def calc_time_m2(instances, params=DEFAULT_PARAMS):
    distr_caching_precomputed = distr_maker(shape=params['cache_skew'], size=params['total_reads'])
    distr_cache = model_distr_split_fn(distr_caching_precomputed, params['first_read_from_s3'])
    instances = calc_time_for_config_m2(instances, distr_cache, params['cpu_h'])

    return instances

