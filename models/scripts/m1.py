from ..const import CPU_H, TOTAL_READS


def calc_time_for_config(inst):
    cpu_time = CPU_H / inst['vcpu.value.count']
    scan_time = TOTAL_READS / (0.8 * inst['network_performance.value.Gib'])

    inst['execution_time_m1'] = scan_time + cpu_time
