import numpy as np
import pandas as pd
from sqlalchemy import text

from preprocessing.instances import instSet_transform
from .m4 import calc_time_m4
from ..snowdb_conn import engine
from ..utils import distr_maker, model_distr_pack

SNOWFLAKE_INSTANCE = instSet_transform()
SNOWFLAKE_INSTANCE = SNOWFLAKE_INSTANCE[SNOWFLAKE_INSTANCE["id"] == "c5d.24xlarge"]


def snowset_warehouse_random(fraction=0.1):
    sql_statement = text(f"SELECT warehouseId FROM snowset TABLESAMPLE SYSTEM ({fraction})")
    result_df = None
    with engine.connect() as conn:
        result_df = pd.DataFrame(conn.execute(sql_statement).fetchall(), columns=['warehouseId'])

    return result_df


def snowset_sample_warehouse(fraction=0.01):
    df_types = {
        'warehouse_id': 'int64',
        'cpu_micros': 'int64',
        'scan_s3': 'int64',
        'scan_cache': 'int64',
        'spool_ssd': 'int64',
        'spool_s3': 'int64',
        'warehouse_size': 'int64'
    }

    sql_statement = text(f"""
       SELECT warehouseId,
       sum(systemCpuTime) + sum(userCpuTime) AS cpu_micros,
       sum(persistentReadBytesS3)            AS scan_s3,
       sum(persistentReadBytesCache)         AS scan_cache,
       sum(intDataReadBytesLocalSSD)         AS spool_ssd,
       sum(intDataReadBytesS3)               AS spool_s3,
       avg(warehouseSize)                    AS warehouse_size
      FROM snowset TABLESAMPLE SYSTEM ({fraction})
      WHERE warehouseSize = 1
      group by warehouseId
    """)

    result_df = None
    with engine.connect() as conn:
        result_df = pd.DataFrame(conn.execute(sql_statement).fetchall(), columns=[
            'warehouse_id',
            'cpu_micros',
            'scan_s3',
            'scan_cache',
            'spool_ssd',
            'spool_s3',
            'warehouse_size'
        ]).astype(df_types)

    return result_df


def snowset_estimate_cache_skew(row):
    scanned = (row['scan_s3'] + row['scan_cache'])/ row['warehouse_size'] / 1024**3
    tail = row['scan_s3'] / row['warehouse_size'] / 1024**3

    bins = {
        'data_mem': pd.DataFrame(data={'size': SNOWFLAKE_INSTANCE['calc_mem_caching'].round(decimals=0), 'prio': SNOWFLAKE_INSTANCE['calc_mem_speed']}),
        'data_sto': pd.DataFrame(data={'size': SNOWFLAKE_INSTANCE['calc_sto_caching'].round(decimals=0), 'prio': SNOWFLAKE_INSTANCE['calc_sto_speed']}),
        'data_s3': pd.DataFrame(data={'size': scanned, 'prio': SNOWFLAKE_INSTANCE['calc_net_speed']})
    }

    skew = 0.00001
    error = 1
    i = 1

    while error > 0.01 and i < 101 and skew > 0:
        distribution = distr_maker(skew, round(scanned))
        pack = model_distr_pack(bins, distribution)
        if pack.iloc[0]["data_s3"] == 0:
            break

        err_abs = round(pack.iloc[0]['data_s3'] - float(tail),2)
        error = round(abs(err_abs/tail), 2)
        skew = skew + np.sign(err_abs) * min(0.1, error / (i * 0.5))
        i += i

    row['data_scan'] = scanned
    row['cache_skew_tail'] = tail
    row['cache_skew'] = skew
    row['cache_skew_error'] = error
    row['cache_skew_iter'] = i

    return row


def snowset_spool_frac_estimation(row):
    scanned = row['scan_s3'] + row['scan_cache']
    spooled = row['spool_s3'] + row['spool_ssd']
    if scanned:
        frac = spooled / scanned
    else:
        frac = 0
    row['spool_frac'] = frac

    return row


def snowset_row_est_spool_skew(row):
    scanned = row['spool_frac'] * row['data_scan']
    tail = row['spool_s3'] / row['warehouse_size'] / 1024 ** 3

    if scanned < 1:
        row['spool_skew_tail'] = tail
        row['spool_skew'] = 0.0001
        row['spool_skew_error'] = 0
        row['spool_skew_iter'] = 0

        return row

    bins = {
        'data_mem': pd.DataFrame(data={'size': SNOWFLAKE_INSTANCE['calc_mem_caching'].round(decimals=0), 'prio': SNOWFLAKE_INSTANCE['calc_mem_speed']}),
        'data_sto': pd.DataFrame(data={'size': SNOWFLAKE_INSTANCE['calc_sto_caching'].round(decimals=0), 'prio': SNOWFLAKE_INSTANCE['calc_sto_speed']}),
        'data_s3': pd.DataFrame(data={'size': scanned, 'prio': SNOWFLAKE_INSTANCE['calc_net_speed']})
    }

    skew = 0.00001
    error = 1
    iter_count = 1

    while error > 0.01 and iter_count < 101 and skew > 0:
        dist_est = distr_maker(skew, round(scanned))
        pack = model_distr_pack(bins, dist_est)

        if not pack.iloc[0]['data_s3'] or tail == 0:
            break

        print("Data S3: ", pack.iloc[0]['data_s3'])
        print("Tail: ", tail)
        err_abs = pack.iloc[0]['data_s3'] - tail
        error = abs(err_abs / tail)
        skew = skew + np.sign(err_abs) * min(0.1, error / (iter_count * 0.5))
        iter_count += 1

    if iter_count >= 100:
        print(["Aborted after 100 iterations, skew might not be very accurate", skew])
    if skew <= 0:
        print(["Skew < 0, this is a weird row.", skew])

    row['spool_skew_tail'] = tail
    row['spool_skew'] = skew
    row['spool_skew_error'] = error
    row['spool_skew_iter'] = iter_count

    return row


def generate_params_from_snowflake(snowflake_data):
    return {
        'query_id': snowflake_data['query_id'],
        'cpu_h': snowflake_data['cpu_micros'] / 10**6 / 60**2,
        'total_reads': snowflake_data['scan_s3'] + snowflake_data['scan_cache'],
        'cache_skew': snowflake_data['cache_skew'],
        'first_read_from_s3': False,
        'spooling_fraction': snowflake_data['spool_frac'],
        'spooling_skew': snowflake_data['spool_skew'],
        'spooling_read_sum':  snowflake_data['spool_frac'] * (snowflake_data['scan_s3'] + snowflake_data['scan_cache']),
        'scaling_param': 0.95,
        'max_instance_count': 128
    }


def calculate_times():
    # query - snowflake_data (params) row
    snowset_subset = snowset_sample_warehouse(0.01)
    snowset_subset = snowset_subset.apply(snowset_estimate_cache_skew, axis=1)
    snowset_subset = snowset_subset.apply(snowset_spool_frac_estimation, axis=1)
    snowset_subset = snowset_subset.apply(snowset_spool_frac_estimation, axis=1)
    queries = generate_params_from_snowflake(snowset_subset)
    for query in queries:
        result = calc_time_m4(SNOWFLAKE_INSTANCE, query)
        result.to_csv("./output/snowflake/query_" + str(query['query_id']) + ".csv")