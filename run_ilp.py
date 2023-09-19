import datetime
import json

from models.const import ModelType, TESTS
from models.scripts.ilp import run_ilp_model
from models.ilp_utils import calc_query_requests, calc_available_instances
from preprocessing.instances import inst_set_transform


if __name__ == "__main__":
    instances = inst_set_transform()
    for params in TESTS:
        query_requests = calc_query_requests(params["queries"], instances)
        available_instances = calc_available_instances(instances, max_instances=params["max_instances"])
        start_time = datetime.datetime.now()
        result = run_ilp_model(
            query_requests,
            available_instances,
            max_instances=params["max_instances"],
            max_queries_per_instance=params["max_queries_per_instance"],
            model_type=ModelType.ILP
        )
        result["total_time"] = (datetime.datetime.now() - start_time).total_seconds()
        with open(f"./output/ilp/{params['output_file']}", "w") as f:
            json.dump(result, fp=f, indent=2)
