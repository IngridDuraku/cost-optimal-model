import pandas as pd

from models.const import DEFAULT_PARAMS
from models.utils import model_distr_pack, distr_maker, model_distr_split_fn


def calc_time_for_config_m3(inst, distr_cache, distr_spooling, cpu_h):
    inst = inst.reset_index()
    bins_cache = {
        'data_mem': pd.DataFrame(data={'size': inst['calc_mem_caching'].round(decimals=0), 'prio': inst['calc_mem_speed']}),
        'data_sto': pd.DataFrame(data={'size': inst['calc_sto_caching'].round(decimals=0), 'prio': inst['calc_sto_speed']}),
        'data_s3': pd.DataFrame(data={'size': [len(distr_cache['working'])] * len(inst), 'prio': inst['calc_net_speed']})
    }

    bins_spooling = {
        'data_mem': pd.DataFrame(data={'size': inst['calc_mem_spooling'].round(decimals=0), 'prio': inst['calc_mem_speed']}),
        'data_sto': pd.DataFrame(data={'size': inst['calc_sto_spooling'].round(decimals=0), 'prio': inst['calc_sto_speed']}),
        'data_s3': pd.DataFrame(data={'size': [len(distr_spooling)] * len(inst), 'prio': inst['calc_net_speed']})
    }

    mem_read_distribution = model_distr_pack(bins_cache, distr_cache['working'])
    spool_read_distribution = model_distr_pack(bins_spooling, distr_spooling)

    cpu_time = cpu_h / inst['vcpu_count']
    inst['data_mem'] = mem_read_distribution['data_mem']
    inst['data_sto'] = mem_read_distribution['data_sto']
    inst['data_s3'] = mem_read_distribution['data_s3']

    inst['spooling_mem'] = spool_read_distribution['data_mem']
    inst['spooling_sto'] = spool_read_distribution['data_sto']
    inst['spooling_s3'] = spool_read_distribution['data_s3']

    scan_time = (mem_read_distribution['data_mem'] + 2 * spool_read_distribution['data_mem']) / inst['calc_mem_speed'] \
                + (mem_read_distribution['data_sto'] + 2 * spool_read_distribution['data_sto']) / inst['calc_sto_speed'] \
                + (mem_read_distribution['data_s3'] + 2 * spool_read_distribution['data_s3']) / inst['calc_net_speed']
    inst['execution_time_m3'] = cpu_time + scan_time

    return inst


def calc_time_m3(instances, params=DEFAULT_PARAMS):
    distr_caching_precomputed = distr_maker(shape=params['cache_skew'], size=params['total_reads'])
    distr_cache = model_distr_split_fn(distr_caching_precomputed, params['first_read_from_s3'])
    spooling_distr = distr_maker(shape=params['spooling_skew'], size=params['spooling_read_sum'])
    instances = calc_time_for_config_m3(instances, distr_cache, spooling_distr, params['cpu_h'])

    return instances
