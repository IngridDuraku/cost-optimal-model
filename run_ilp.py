import json

from models.const import TESTS, TSTS
from models.scripts.ilp import calc_query_requests, calc_available_instances, run_ilp_model, prepare_tests
from models.scripts.ilp_bw import run_ilp_bw_model
from preprocessing.instances import inst_set_transform
import datetime


if __name__ == "__main__":
    instances = inst_set_transform()
    for batch_size in range(5, 6):
        ilp_results = []
        ilp_bw_results = []
        for params in prepare_tests(batch_size):
            query_requests = calc_query_requests(params["queries"], instances)
            print(query_requests)
            available_instances = calc_available_instances(instances, max_instances=params["max_instances"])
            start_time = datetime.datetime.now()
            ilp_result = run_ilp_model(
                query_requests,
                available_instances,
                max_instances=params["max_instances"],
                max_queries_per_instance=params["max_queries_per_instance"],
                output_file=params["output_file"]
            )
            ilp_result["total_time"] = (datetime.datetime.now() - start_time).total_seconds()
            ilp_results.append(ilp_result)
            with open(f"./ilp_output/test_snowflake_{batch_size}.json", "w") as f:
                json.dump(ilp_results, fp=f, indent=2)
            start_time = datetime.datetime.now()
            ilp_bw_result = run_ilp_bw_model(
                query_requests,
                available_instances,
                max_instances=params["max_instances"],
                max_queries_per_instance=params["max_queries_per_instance"],
                output_file=params["output_file"]
            )
            ilp_bw_result["total_time"] = (datetime.datetime.now() - start_time).total_seconds()
            ilp_bw_results.append(ilp_bw_result)
            with open(f"./ilp_bw_output/test_snowflake_test_{batch_size}.json", "w") as f:
                json.dump(ilp_bw_results, fp=f, indent=2)

