
from preprocessing.instances import instSet_transform
from models.scripts.simulated_annealing import simulate_annealing
from models.const import QUERIES


if __name__ == '__main__':
    instances = instSet_transform()
    instances.to_csv("./input/input_" + ".csv")

    batch_cost_sum = 0
    individually_optimized_cost_sum = 0

    for t0 in range(6_000, 10_001, 2_000):
        print("t0: " + str(t0))
        for i in range(7):
            print("seed: " + str(i))
            batch_optimized_cost, individually_optimized_cost = simulate_annealing(instances.copy(), QUERIES, 500, t0, i)
            batch_cost_sum += batch_optimized_cost
            individually_optimized_cost_sum += individually_optimized_cost
            print(batch_cost_sum / individually_optimized_cost_sum)
        
