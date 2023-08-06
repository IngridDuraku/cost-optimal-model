from models.scripts.m4 import calc_time_m4

def calc_time_batch(instances, queries):
    result = None
    for query in queries:
        intermediate_result = calc_time_m4(instances, query)
        if result == None:
            result = intermediate_result
        else:
            for i in range(len(result)):
                intermediate_result[i]['id_name'] = ""
                intermediate_result[i]['count'] = 0
                intermediate_result[i]['id'] = ""
                intermediate_result[i]['cost_usdph'] = 0
                intermediate_result[i]['scale'] = 0
                result[i] = result[i].add(intermediate_result[i], axis='columns')
    return result