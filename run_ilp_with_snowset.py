import json

from models.const import ModelType
from models.scripts.ilp import run_ilp_model
from models.ilp_utils import calc_query_requests, calc_available_instances, prepare_tests_from_snowflake
from preprocessing.instances import inst_set_transform
import datetime


if __name__ == "__main__":
    instances = inst_set_transform()
    for batch_size in range(2, 3):
        ilp_results = []
        ilp_bw_results = []
        for params in prepare_tests_from_snowflake(batch_size):
            query_requests = calc_query_requests(params["queries"], instances)
            print(query_requests)
            available_instances = calc_available_instances(instances, max_instances=params["max_instances"])
            start_time = datetime.datetime.now()
            ilp_result = run_ilp_model(
                query_requests,
                available_instances,
                max_instances=params["max_instances"],
                max_queries_per_instance=params["max_queries_per_instance"],
                model_type=ModelType.ILP
            )
            ilp_result["total_time"] = (datetime.datetime.now() - start_time).total_seconds()
            ilp_results.append(ilp_result)
            with open(f"./output/ilp_with_snowset/ilp/test_snowflake_{batch_size}.json", "w") as f:
                json.dump(ilp_results, fp=f, indent=2)
            start_time = datetime.datetime.now()
            ilp_bw_result = run_ilp_model(
                query_requests,
                available_instances,
                max_instances=params["max_instances"],
                max_queries_per_instance=params["max_queries_per_instance"],
                model_type=ModelType.ILP_BW
            )
            ilp_bw_result["total_time"] = (datetime.datetime.now() - start_time).total_seconds()
            ilp_bw_results.append(ilp_bw_result)
            with open(f"./output/ilp_with_snowset/ilp_bw/test_snowflake_{batch_size}.json", "w") as f:
                json.dump(ilp_bw_results, fp=f, indent=2)

