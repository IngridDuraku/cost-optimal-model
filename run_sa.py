from preprocessing.instances import inst_set_transform
from models.scripts.simulated_annealing import simulate_annealing, prepare_tests
import json

if __name__ == '__main__':
    instances = inst_set_transform()
    result = {}
    for params in prepare_tests():
            print("seed: " + str(params["seed"]))
            print("t0: " + str(params["t0"]))
            print("iterations: " + str(params["iterations"]))
            print("max per instance: " + str(params["max_per_instance"]))
            print("provision probability: " + str(params["provision_prob"]))
            cost_sa, cost_separated = simulate_annealing(
                instances.copy(),
                params["queries"],
                params["iterations"],
                params["t0"],
                params["seed"],
                params["max_per_instance"],
                params["provision_prob"]
            )
            print("cost sa: " + str(cost_sa))
            print("cost separated: " + str(cost_separated))
            if params["t0"] not in result:
                  result[params["t0"]] = {}
            if params["iterations"] not in result[params["t0"]]:
                  result[params["t0"]][params["iterations"]] = {}
            result[params["t0"]][params["iterations"]][params["provision_prob"]] = {
                  "cost_sa": cost_sa,
                  "cost_separated": cost_separated
            }
            if params["provision_prob"] == 0.5:
                  with open(params["output_file"], 'w') as fp:
                        json.dump(result, fp)
            

    print("Done")