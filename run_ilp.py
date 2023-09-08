from models.const import TESTS
from models.scripts.ilp import calc_query_requests, calc_available_instances, run_ilp_model
from preprocessing.instances import inst_set_transform


if __name__ == "__main__":
    instances = inst_set_transform()
    for params in TESTS:
        query_requests = calc_query_requests(params["queries"], instances)
        available_instances = calc_available_instances(instances, max_instances=params["max_instances"])
        run_ilp_model(
            query_requests,
            available_instances,
            max_instances=params["max_instances"],
            max_queries_per_instance=params["max_queries_per_instance"],
            output_file=params["output_file"]
        )

    print("Done")
