from ..const import DEFAULT_PARAMS


def calc_time_for_config(instances, params=DEFAULT_PARAMS):
    cpu_time = params['cpu_h'] / instances['vcpu_count']
    scan_time = params['total_reads'] / (0.8 * instances['network_Gbps'])

    instances['execution_time_m1'] = scan_time + cpu_time
