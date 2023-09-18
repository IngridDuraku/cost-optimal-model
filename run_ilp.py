import json

from models.const import TESTS, TSTS
from models.scripts.ilp import calc_query_requests, calc_available_instances, run_ilp_model, prepare_tests
from models.scripts.ilp_bw import run_ilp_bw_model
from preprocessing.instances import inst_set_transform
import datetime
from snowflake import SNOWFLAKE_INSTANCE


if __name__ == "__main__":
    instances = SNOWFLAKE_INSTANCE
    print(instances)
    for max_time in range(20, 400, 20):
        ilp_results = []
        ilp_bw_results = []
        status = 0
        for params in prepare_tests(8):
            query_requests = calc_query_requests(params["queries"], instances)
            #print(query_requests)
            available_instances = calc_available_instances(instances, max_instances=params["max_instances"])
            start_time = datetime.datetime.now()
            ilp_result = run_ilp_model(
                query_requests,
                available_instances,
                params["max_queries_per_instance"],
                params["max_instances"],
                max_time
            )
            ilp_result["total_time"] = (datetime.datetime.now() - start_time).total_seconds()
            ilp_results.append(ilp_result)
            status += ilp_result["status"]
            with open(f"./ilp_output/batch_size_8_different_limits_snowflake/different_limit_{max_time}.json", "w") as f:
                json.dump(ilp_results, fp=f, indent=2)
            start_time = datetime.datetime.now()
            ilp_bw_result = run_ilp_bw_model(
                query_requests,
                available_instances,
                params["max_queries_per_instance"],
                params["max_instances"],
                max_time
            )
            ilp_bw_result["total_time"] = (datetime.datetime.now() - start_time).total_seconds()
            ilp_bw_results.append(ilp_bw_result)
            status += ilp_bw_result["status"]
            with open(f"./ilp_bw_output/batch_size_8_different_limits_snowflake/different_limit_{max_time}.json", "w") as f:
                json.dump(ilp_bw_results, fp=f, indent=2)
        if status == 0:
            print("finished at max time: " + str(max_time))
            break

