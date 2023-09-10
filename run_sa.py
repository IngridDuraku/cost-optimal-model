from preprocessing.instances import inst_set_transform
from models.scripts.simulated_annealing import simulate_annealing, prepare_tests


if __name__ == '__main__':
    instances = inst_set_transform()
    for params in prepare_tests():
            print("seed: " + str(params["seed"]))
            print("t0: " + str(params["t0"]))
            simulate_annealing(
                instances.copy(),
                params["queries"],
                params["iterations"],
                params["t0"],
                params["seed"]
            )

    print("Done")