#!/usr/bin/env python
# coding: utf-8

# Import and preprocess data
# aws_historical_data_new

import pandas as pd
from pandasql import sqldf

pd.set_option('display.max_columns', 500)

def storage_type(row):
    if pd.isna(row['storage_size']):
        return 'EBS'
    elif row['storage_size'] == 0:
        return 'EBS'
    elif row['storage_nvme_ssd'] and not pd.isna(row['storage_nvme_ssd']):
        return 'NVMe'
    elif row['storage_ssd'] and not pd.isna(row['storage_ssd']):
        return 'SSD'
    else:
        return 'HDD'

# original also checks for instance type starts with a1
# but these instances are already filtered
def CPU_brand(row):
    if 'AMD' in row['physical_processor']:
        return 'AMD'
    elif 'Intel' in row['physical_processor']:
        return 'Intel'
    else:
        return '?'

def aws_data_historical_new_load():
    dates = pd.read_csv('data/historical-data-times.csv')
    data = pd.read_csv('data/historical-data-raw.csv', skipinitialspace=True)

    # translation of sql query where clause
    data.dropna(subset=['vCPU'], inplace=True)
    data.drop(data[data['generation'] != 'current'].index, inplace=True)
    data.drop(data[data['network_performance'].isin(['High', 'Moderate', 'Low', 'Very Low', 'Very High'])].index, inplace=True)
    data.drop(data[data['GPU'].notna() & data['GPU'] != 0].index, inplace=True)
    data.drop(data[data['FPGA'].notna() & data['FPGA'] != 0].index, inplace=True)
    data.drop(data[data['instance_type'].str.contains('^a1', regex=True)].index, inplace=True) # ARM
    data.drop(data[data['instance_type'].str.contains('.metal$', regex=True)].index, inplace=True)
    data.drop(data[data['instance_type'].str.contains('^t2', regex=True)].index, inplace=True) # burst
    data.drop(data[data['instance_type'].str.contains('^t3', regex=True)].index, inplace=True) # burst
    # translation of sql query join
    data = data.join(dates.set_index('entry'), on='entry', how='inner')

    # translation of sql query new fields
    data['storage_type'] = data.apply(storage_type, axis=1)
    # why are we doing this here if we're just dropping the columns later?
    # keep the columns anyway because then we don't need complicaetd regular expressions later
    split_instance_types = data['instance_type'].str.split('.', n=1, expand=True)
    split_instance_types = split_instance_types.rename(columns={0: 'instance_prefix', 1: 'instance_suffix'})
    data = pd.concat([data, split_instance_types], axis=1)
    data['physical_processor'] = data['physical_processor'].astype(str)
    data['CPU_brand'] = data.apply(CPU_brand, axis=1)

    # translation of transmute
    data['id'] = data['instance_type'].astype(str)
    data['vcpu_value_count'] = data['vCPU']
    data['memory_value_gib'] = data['memory']
    data['clockSpeed_value_ghz'] = pd.to_numeric(data['clock_speed_ghz'].str.replace('GHz', '', case=False))
    data['storage_sum_gib'] = data.apply(lambda row: row['storage_size'] * row['storage_devices'], axis=1)
    data['storage_sum_gib'] = data['storage_sum_gib'].fillna(0)
    data['storage_count'] = data['storage_devices'].fillna(value=0)
    # keep storage_type
    # keep network_performance
    data['network_performance_value_Gib'] = data['network_performance'].str.replace("Up to", "", case=False)
    data['network_performance_value_Gib'] = data['network_performance_value_Gib'].str.replace("Gigabit", "", case=False)
    data['network_performance_value_Gib'] = data['network_performance_value_Gib'].str.replace("Gbps", "", case=False)
    data['network_performance_value_Gib'] = data['network_performance_value_Gib'].str.replace("Gpbs", "", case=False)
    data['network_performance_value_Gib'] = pd.to_numeric(data['network_performance_value_Gib'])
    data['is_guaranteed'] = ~data["network_performance"].str.contains("Up to", case=False)
    data['cost_ondemand_value_usdph'] = data['pricing']
    data['processorName'] = data['physical_processor']
    data['clockSpeed_text'] = data['clock_speed_ghz']
    data['region_name'] = "us-east-1"
    data['join_entry'] = data['entry'] - 1
    data['join_time'] = data['time']
    
    data = data[['id', 'vcpu_value_count', 'memory_value_gib', 'clockSpeed_value_ghz', 'storage_sum_gib',
                 'storage_count', 'storage_type', 'network_performance', 'network_performance_value_Gib',
                 'is_guaranteed', 'cost_ondemand_value_usdph', 'processorName', 'clockSpeed_text', 'region_name',
                 'join_entry', 'join_time',
                 'instance_prefix', 'instance_suffix']] # added by me
    return data


aws_data_historical_new_load()


# ## aws_data_all

def aws_data_cleanup(data):
    data.loc[pd.isna(data['clock_ghz']), ['loading_comment', 'clock_ghz']] = ["Clock speed unkown, assuming default value of 2.5 GHz", 2.5]
    return data

def aws_data_normalize(data):
    # translation of transmute
    # keep id
    data['memory_Gib'] = data['memory_value_gib']
    data['vcpu_count'] = data['vcpu_value_count']
    data['clock_ghz'] = data['clockSpeed_value_ghz']
    data['storage_Gib'] = data['storage_sum_gib']
    # keep storage_count
    # keep storage_type
    data['network_Gbps'] = data['network_performance_value_Gib']
    data['network_is_steady'] = data['is_guaranteed']
    data['cost_usdph'] = data['cost_ondemand_value_usdph']
    data['meta_region_name'] = data['region_name']
    data['meta_join_entry'] = data['join_entry']
    data['meta_join_time'] = data['join_time']
    data['loading_comment'] = ""
    data = data[['id', 'memory_Gib', 'vcpu_count', 'clock_ghz', 'storage_Gib', 'storage_count', 'storage_type',
                'network_Gbps', 'network_is_steady', 'cost_usdph', 'meta_region_name', 'meta_join_entry',
                'meta_join_time', 'loading_comment',
                 'instance_prefix', 'instance_suffix']] # added by me
    # cleanup function
    data = aws_data_cleanup(data)
    # join  with commits
    commits = pd.read_csv('data/ec2-instances.info-commit-mapping.csv')
    data = data.join(commits.set_index('join.entry'), on='meta_join_entry', how='inner')
    return data

def aws_data_enhance_ids(data):
    # use instance_prefix and instance_suffix columns instead of complicated regular expressions
    data['id_prefix'] = data['instance_prefix']
    # first numbers of instance_suffix.
    # originally only 1-9: 10 -> 1
    # doesn't make sense to me
    data['id_numstr'] = data['instance_suffix'].str.extract(r'^(\d+)', expand=False)
    data['id_number'] = pd.to_numeric(data['id_numstr'], errors='coerce').fillna(0)
    data = data.drop(columns=['instance_prefix', 'instance_suffix'])
    return data

def add_slice_info(data, largest):
    def f(row):
        largest_index = largest[row['meta_join_entry']][row['id_prefix']]
        largest_row = data.loc[largest_index]
        # id_slice
        if not row['id_number'] == 0:
            row['id_slice'] = row['id_number']
        elif 'metal' in row['id']:
            row['id_slice'] = largest_row['id_number']
        elif 'xlarge' in row['id']:
            row['id_slice'] = 1
        else:
            row['id_slice'] = 0.5

        # id_slice_factor
        if (largest_row['id_number'] == 0):
            row['id_slice_factor'] = 1
        else:
            row['id_slice_factor'] = row['id_slice'] / largest_row['id_number']  

        row['id_slice_of'] = largest_index
        row['id_slice_net'] = largest_row['network_Gbps'] * row['id_slice_factor']
        row['id_slice_sto'] = largest_row['storage_count']
        return(row)
    return f

def aws_data_with_prefixes(data):
    grouped_data = data.groupby(['meta_join_entry', 'id_prefix'])
    # find largest per group
    largest = grouped_data['id_number'].idxmax()
    data = data.apply(add_slice_info(data, largest), axis=1)
    return data

def aws_data_all_transform():
    data = aws_data_historical_new_load()
    data = aws_data_normalize(data)
    data = aws_data_enhance_ids(data)
    data = aws_data_with_prefixes(data)
    data['meta_origin'] = 'instances.json'
    return data

# aws_data_all_by_date

def aws_data_all_by_date_transform():
    data = aws_data_all_transform()
    data['meta_group'] = data['commit.date'].astype(str) + ' | ' + data['meta_join_entry'].astype(str) + \
                            ' | ' + data['meta_origin']
    data.sort_values(by=['meta_join_entry', 'meta_origin'], ascending=False, inplace=True)
    return data

# ui_instance_sets

def ui_instance_sets_transform(data):
    # do something
    return data
# not really needed right now. we can get instSet.all directly from aws_data_all_by_date
#ui_instance_sets = ui_instance_sets_transform(aws_data_all_by_date)
#ui_instance_sets

# instSet_all

input_instanceSet = '2020-06-02 | 102 | instances.json'

def instSet_all_transform():
    data = aws_data_all_by_date_transform()
    grouped_data = data.groupby(['meta_group']) 
    data = grouped_data.get_group(input_instanceSet)
    return data

# instSet_long

paper_inst_ids = ["c5n.18xlarge", "c5.24xlarge", "z1d.12xlarge", "c5d.24xlarge",
 "m5.24xlarge", "i3.16xlarge", "m5d.24xlarge", "m5n.24xlarge",
 "r5.24xlarge", "m5dn.24xlarge", "r5d.24xlarge", "r5n.24xlarge",
 "r5dn.24xlarge", "i3en.24xlarge", "x1e.32xlarge"]

def aws_data_filter_paper(data):
    return data.loc[data['id'].isin(paper_inst_ids)]

# only implemented default filter
instanceFilter = [aws_data_filter_paper]

def calc_net_speed(row):
    return (row['network_Gbps'] if row['network_is_steady'] else row['id_slice_net']) / 8

model_factors_bandwidth = {
    'RAM': 50,
    'NVMe': 2,
    'SSD': 0.5,
    'HDD': 0.25,
    'EBS': float('nan')
}

def model_calc_storage_speed(data):
    bws = data['storage_type'].map(model_factors_bandwidth) * data['id_slice_sto'] * data['id_slice_factor']
    return bws.fillna(data['calc_net_speed'])

def model_with_speeds(data):
    data['calc_net_speed'] = data.apply(calc_net_speed, axis=1)
    data['calc_s3_speed'] = data['calc_net_speed'] * 0.8
    data['calc_mem_speed'] = model_factors_bandwidth['RAM']
    data['calc_sto_speed'] = model_calc_storage_speed(data)
    ## no hyperthreads, assume 2 threads/core
    data['calc_cpu_real'] = data['vcpu_count'] / 2
    data['calc_mem_caching'] = data['memory_Gib'] / 2
    data['calc_sto_caching'] = data['storage_Gib'] / 2
    data['calc_mem_spooling'] = data['memory_Gib'] - data['calc_mem_caching']
    data['calc_sto_spooling'] = data['storage_Gib'] - data['calc_sto_caching']
    return data

def instSet_long_transform():
    data = instSet_all_transform()
    for function in instanceFilter:
        data = function(data)
    data = data.dropna(axis=0)
    data = model_with_speeds(data)
    return data

# instSet

def instSet_transform():
    data = instSet_long_transform()
    data.drop(list(data.filter(regex = '^meta')), axis = 1, inplace = True)
    data.drop(list(data.filter(regex = '^commit')), axis = 1, inplace = True)
    return data

