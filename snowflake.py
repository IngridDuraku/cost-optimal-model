import numpy as np
import pandas as pd
from sqlalchemy import text

from preprocessing.instances import inst_set_transform
from models.snowdb_conn import engine
from models.utils import distr_maker, model_distr_pack

SNOWFLAKE_INSTANCE = inst_set_transform()
SNOWFLAKE_INSTANCE = SNOWFLAKE_INSTANCE[SNOWFLAKE_INSTANCE["id"] == "c5d.2xlarge"]


def snowset_warehouse_random(fraction=0.1):
    sql_statement = text(f"SELECT warehouseId FROM snowset TABLESAMPLE SYSTEM ({fraction})")
    result_df = None
    with engine.connect() as conn:
        result_df = pd.DataFrame(conn.execute(sql_statement).fetchall(), columns=['warehouseId'])

    return result_df


def snowset_sample_warehouse(fraction=0.01):
    df_types = {
        'warehouse_id': 'int64',
        'query_id': 'int64',
        'cpu_micros': 'int64',
        'scan_s3': 'int64',
        'scan_cache': 'int64',
        'spool_ssd': 'int64',
        'spool_s3': 'int64',
        'warehouse_size': 'int64',
        'total_duration': 'int64',
        'max_cores': 'int64',
    }

    sql_statement = text(f"""
       SELECT warehouseId as warehouse_id,
       queryId as query_id,
       systemCpuTime + userCpuTime AS cpu_micros,
       persistentReadBytesS3           AS scan_s3,
       persistentReadBytesCache         AS scan_cache,
       intDataReadBytesLocalSSD         AS spool_ssd,
       intDataReadBytesS3              AS spool_s3,
       warehouseSize                    AS warehouse_size,
       durationtotal                    AS total_duration,
       perServerCores                    AS max_cores
      FROM snowset TABLESAMPLE SYSTEM ({fraction})
      WHERE warehouseSize = 1
    """)

    result_df = None
    with engine.connect() as conn:
        result_df = pd.DataFrame(conn.execute(sql_statement).fetchall(), columns=[
            'warehouse_id',
            'query_id',
            'cpu_micros',
            'scan_s3',
            'scan_cache',
            'spool_ssd',
            'spool_s3',
            'warehouse_size',
            'total_duration',
            'max_cores'
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
    return pd.DataFrame({
        'warehouse_id': snowflake_data['warehouse_id'],
        'query_id': snowflake_data['query_id'],
        'total_duration': snowflake_data['total_duration']/1000,
        'cpu_h': snowflake_data['cpu_micros'] / 10**6 / 60**2,
        'total_reads': (snowflake_data['scan_s3'] + snowflake_data['scan_cache']) / 1024**3,
        'cache_skew': snowflake_data['cache_skew'],
        'first_read_from_s3': False,
        'spooling_fraction': snowflake_data['spool_frac'],
        'spooling_skew': snowflake_data['spool_skew'],
        'spooling_read_sum':  snowflake_data['spool_frac'] * ((snowflake_data['scan_s3'] + snowflake_data['scan_cache'])/ 1024**3),
        'scaling_param': 0.95,
        'max_cores': snowflake_data['max_cores'],
        'max_instance_count': 1
    })


if __name__ == "__main__":
    # generate snowflake queries
    snowset_subset = snowset_sample_warehouse(0.01)
    snowset_subset = snowset_subset[
        snowset_subset["scan_s3"] != 0 &
        (snowset_subset["scan_cache"] > (SNOWFLAKE_INSTANCE.iloc[0]["calc_sto_caching"] + SNOWFLAKE_INSTANCE.iloc[0][
            "calc_mem_caching"]) * 1024 ** 3) &
        (snowset_subset['scan_s3'] + snowset_subset['scan_cache'] > snowset_subset['warehouse_size'] * (
                    300 * 1024 ** 3))
        ]
    snowset_subset = snowset_subset.apply(snowset_estimate_cache_skew, axis=1)
    snowset_subset = snowset_subset.apply(snowset_spool_frac_estimation, axis=1)
    snowset_subset = snowset_subset.apply(snowset_row_est_spool_skew, axis=1)
    queries = generate_params_from_snowflake(snowset_subset)

    queries.to_csv("./input/snowflake_queries.csv")

