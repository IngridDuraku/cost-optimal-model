import numpy as np
import pandas as pd
from sqlalchemy import text

from preprocessing.instances import instSet_transform
from ..snowdb_conn import engine
from ..utils import distr_maker, model_distr_pack


def snowset_warehouse_random(fraction = 0.1):
    sql_statement = text(f"SELECT warehouseId FROM snowset TABLESAMPLE SYSTEM ({fraction})")
    result_df = None
    with engine.connect() as conn:
        result_df = pd.DataFrame(conn.execute(sql_statement).fetchall(), columns=['warehouseId'])

    return result_df


def snowset_sample_warehouse(fraction=0.1):
    sql_statement = text("""
       SELECT s.warehouse_id,
       sum(systemCpuTime) + sum(userCpuTime) AS cpu_micros,
       sum(persistentReadBytesS3)            AS scan_s3,
       sum(persistentReadBytesCache)         AS scan_cache,
       sum(intDataReadBytesLocalSSD)         AS spool_ssd,
       sum(intDataReadBytesS3)               AS spool_s3,
       avg(warehousesize)                    AS warehouse_size
      FROM snowset s
      JOIN (SELECT warehouseId FROM snowset TABLESAMPLE SYSTEM ({fraction})) w ON w.warehouseId = s.warehouseId
      WHERE s.warehouseSize = 1
      GROUP BY s.warehouseId
    """)

    result_df = None
    with engine.connect() as conn:
        result_df = pd.DataFrame(conn.execute(sql_statement).fetchall(), columns=[
            'cpu_micros',
            'scan_s3',
            'scan_cache',
            'spool_ssd',
            'spool_s3',
            'warehouse_size'
        ])

    return result_df


def snowset_estimate_cache_skew(row):
    snowflake_instance = instSet_transform()[id == "c5d.2xlarge"]
    scanned = row['scan_s3'] + row['scan_cache'] / row['warehouse_size'] / 1024**3
    tail = row['scan_s3'] / row['warehouse_size'] / 1024**3

    bins = {
        'data_mem': pd.DataFrame(data={'size': snowflake_instance['calc_mem_caching'].round(decimals=0), 'prio': snowflake_instance['calc_mem_speed']}),
        'data_sto': pd.DataFrame(data={'size': snowflake_instance['calc_sto_caching'].round(decimals=0), 'prio': snowflake_instance['calc_sto_speed']}),
        'data_s3': pd.DataFrame(data={'size': scanned, 'prio': snowflake_instance['calc_net_speed']})
    }

    skew = 0.00001
    error = 1
    i = 0

    while error > 0.01 and i < 100 and skew > 0:
        distribution = distr_maker(skew, round(scanned))
        pack = model_distr_pack(bins, distribution)
        if not pack["data_s3"]:
            break

        err_abs = pack['data_s3'] - tail
        error = abs(err_abs/tail)
        skew = skew + np.sign(err_abs) * np.min(0.1, error / (i * 0.5))
        i += i

    # todo: check accuracy

    row['data_scan'] = scanned
    row['cache_skew_tail'] = tail
    row['cache_skew'] = skew
    row['cache_skew_error'] = error
    row['cache_skew_iter'] = i

    return row


def snowset_spool_frac_estimation(row):
    scanned = row['scan_s3'] + row['scan_cache']
    spooled = row['spool_s3'] + row['spool_ssd']
    row['spool_frac'] = spooled / scanned


def snowset_row_est_spool_skew(row, df):
    snowflake_instance = instSet_transform()[id == "c5d.2xlarge"]
    scanned = row['spool_frac'] * row['data_scan']
    tail = row['spool_s3'] / row['warehouse_size'] / 1024 ** 3

    if scanned < 1:
        row['spool_skew_tail'] = tail
        row['spool_skew'] = 0.0001
        row['spool_skew_error'] = 0
        row['spool_skew_iter'] = 0

        return row

    bins = {
        'data_mem': pd.DataFrame(data={'size': snowflake_instance['calc_mem_caching'].round(decimals=0), 'prio': snowflake_instance['calc_mem_speed']}),
        'data_sto': pd.DataFrame(data={'size': snowflake_instance['calc_sto_caching'].round(decimals=0), 'prio': snowflake_instance['calc_sto_speed']}),
        'data_s3': pd.DataFrame(data={'size': scanned, 'prio': snowflake_instance['calc_net_speed']})
    }

    skew = 0.00001
    error = 1
    iter_count = 0

    while error > 0.01 and iter_count < 100 and skew > 0:
        dist_est = distr_maker(skew, round(scanned))
        pack = model_distr_pack(bins, dist_est)

        if not pack['data_s3']:
            break

        err_abs = pack['data_s3'] - tail
        error = np.abs(err_abs / tail)
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






