import pandas as pd
import numpy as np
from scipy.stats import zipf
from ..const import CPU_H


def distr_maker(shape, size):
    if np.isnan(size):
        return []
    if size <= 1:
        return [size]

    distr = zipf.pmf(np.arange(1, size+1), shape)
    normd = distr / np.sum(distr) * size
    return normd.tolist()


def model_distr_hsplit(distr, lim):
    dist_low = np.minimum(distr, lim)
    dist_high = np.maximum(distr - dist_low, 0)
    return {'low': dist_low, 'high': dist_high}


def model_distr_split_fn(distr, split_first_read):
    if split_first_read:
        split_dist = model_distr_hsplit(distr, 1)
    else:
        split_dist = [np.zeros(len(distr)), distr] # ToDO: Fix
    return {"initial": split_dist['low'], "working": split_dist['high']}


def calc_inst_speeds(inst):
    pass


def calc_groups(sizes, distr_len):
    if len(sizes) == 1:
        return [sizes.index[0]] * min(sizes[0], distr_len)
    elif sizes[0] > distr_len:
        return []
    else:
        return [sizes.index[1]] * (min(sizes[1], distr_len) - sizes[0])


def distr_pack_helper(bins, distr):
    distr_len = len(distr)
    bins = bins.sort_values(by='prio', ascending=False)
    bins['acc_size'] = bins['size'].cumsum().astype('int32')
    size_windows = bins['acc_size'].rolling(window=2)
    res = []
    for size_window in size_windows:
        res.extend(calc_groups(size_window, distr_len))

    result = pd.DataFrame(data={
        'distr_val': distr,
        'group': res
    }).groupby('group').sum().transpose()

    return result.reset_index()


def model_distr_pack(bins, distr):
    n = len(bins['data_mem']['prio'])
    res = pd.DataFrame()
    for i in range(n):
        next_ = distr_pack_helper(
            bins=pd.DataFrame(
                data={
                    'prio': [bins['data_mem']['prio'][i], bins['data_sto']['prio'][i], bins['data_s3']['prio'][i]],
                    'size': [bins['data_mem']['size'][i], bins['data_sto']['size'][i], bins['data_s3']['size'][i]],
                },
                index=['data_mem', 'data_sto', 'data_s3']
            ),
            distr=distr
        )
        res = pd.concat([res, next_], ignore_index=True).fillna(0)

    return res


def calc_time_for_config(inst, distr_cache):
    data_mem = pd.DataFrame(data={'size': inst['calc_mem_caching'].round(decimals=0), 'prio': inst['calc_mem_speed']})
    data_sto = pd.DataFrame(data={'size': inst['calc_sto_caching'].round(decimals=0), 'prio': inst['calc_sto_speed']})
    data_s3 = pd.DataFrame(data={'size': [len(distr_cache['working'])] * len(instances), 'prio': inst['calc_net_speed']})

    bins_cache = {
        'data_mem': data_mem,
        'data_sto': data_sto,
        'data_s3': data_s3
    }

    mem_read_distribution = model_distr_pack(bins_cache, distr_cache['working'])
    print(mem_read_distribution.dtypes)
    print(len(mem_read_distribution))
    cpu_time = CPU_H / inst['vcpu.value.count']
    inst['data_mem'] = mem_read_distribution['data_mem']
    inst['data_sto'] = mem_read_distribution['data_sto']
    inst['data_s3'] = mem_read_distribution['data_s3']

    scan_time = mem_read_distribution['data_mem'] / inst['calc_mem_speed'] +  mem_read_distribution['data_sto'] / inst['calc_sto_speed'] + mem_read_distribution['data_s3'] / inst['calc_net_speed']
    inst['execution_time_m2'] = cpu_time + scan_time
