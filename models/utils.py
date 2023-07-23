import numpy as np
import pandas as pd
from scipy.stats import zipf


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
    return {'initial': dist_low, 'working': dist_high}


def model_distr_split_fn(distr, split_first_read):
    if split_first_read:
        split_dist = model_distr_hsplit(distr, 1)
    else:
        split_dist = {"initial": np.zeros(len(distr)), "working": distr}
    return split_dist


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

    return sanitize_packing(res)


def model_make_scaling(p, n):
    return (1 - p) + (p / n)


def sanitize_packing(packed_df):
    cols = {"data_mem", "data_sto", "data_s3"}
    for col in cols:
        if col not in packed_df.columns:
            packed_df[col] = 0
    packed_df.round(decimals=0)

    return packed_df
