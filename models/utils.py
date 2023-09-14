import numpy as np
import pandas as pd
from scipy.stats import zipfian


def distr_maker(shape, size):
    if np.isnan(size):
        return []
    if size <= 1:
        return [size]

    distr = zipfian.pmf(np.arange(1, size+1), shape, size+1)
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


# def distr_pack_helper(bins, distr):
#     distr_len = len(distr)
#     bins = bins.sort_values(by='prio', ascending=False)
#     bins['acc_size'] = bins['size'].astype('int32').cumsum()
#     size_windows = bins['acc_size'].rolling(window=2)
#     res = []
#     for size_window in size_windows:
#         res.extend(calc_groups(size_window, distr_len))
#
#     result = pd.DataFrame(data={
#         'distr_val': distr,
#         'group': res
#     }).groupby('group').sum().transpose()
#
#     return result.reset_index()


def distr_pack_helper(bins, distr, index):
    distr_len = len(distr)
    bins = bins.sort_values(by='prio', ascending=False)
    bins['acc_size'] = bins['size'].astype('int32').cumsum()
    size_windows = bins['acc_size'].rolling(window=2)
    res = []
    for size_window in size_windows:
        res.extend(calc_groups(size_window, distr_len))

    intermediate = pd.DataFrame(data={
        'distr_val': distr,
        'group': res,
        'data_read': 1
    }).groupby('group').sum().transpose()

    intermediate = sanitize_packing(intermediate)

    result = pd.DataFrame(data={
        'data_mem': 0 if len(intermediate) == 0 else intermediate.iloc[0]["data_mem"],
        'data_sto': 0 if len(intermediate) == 0 else intermediate.iloc[0]["data_sto"],
        'data_s3': 0 if len(intermediate) == 0 else intermediate.iloc[0]["data_s3"],
        'data_stored_mem': 0 if len(intermediate) == 0 else intermediate.iloc[1]["data_mem"],
        'data_stored_sto': 0 if len(intermediate) == 0 else intermediate.iloc[1]["data_sto"],
        'data_stored_s3': 0 if len(intermediate) == 0 else intermediate.iloc[1]["data_s3"],
    }, index=[index])

    return result


def model_distr_pack(bins, distr):
    n = len(bins['data_mem']['prio'])
    res = pd.DataFrame()
    for i in range(n):
        next_ = distr_pack_helper(
            bins=pd.DataFrame(
                data={
                    'prio': [bins['data_mem']['prio'].iloc[i], bins['data_sto']['prio'].iloc[i], bins['data_s3']['prio'].iloc[i]],
                    'size': [bins['data_mem']['size'].iloc[i], bins['data_sto']['size'].iloc[i], bins['data_s3']['size'].iloc[i]],
                },
                index=['data_mem', 'data_sto', 'data_s3']
            ),
            distr=distr,
            index=bins['data_mem'].index[i]
        )
        res = pd.concat([res, next_], ignore_index=True).fillna(0)

    return res


def model_make_scaling(p, n):
    return (1 - p) + (p / n)


def sanitize_packing(packed_df):
    cols = ["data_mem", "data_sto", "data_s3"]
    result = pd.DataFrame(columns=cols)
    for col in cols:
        if col not in packed_df.columns:
            result[col] = 0
        else:
            result[col] = packed_df[col]
    result.round(decimals=2)

    return result


def calc_cost(time_df):
    result = []
    for instance in time_df:
        instance["stat_price_sum"] = round(instance["stat_time_sum"] * instance["cost_usdph"] / 3600, 10)
        result.append(instance.sort_values(by="stat_price_sum", ascending=True))

    return result
