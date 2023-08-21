from models.utils import calc_cost
from preprocessing.instances import instSet_transform
from models.scripts.batch import Scheduler
from models.const import QUERIES


if __name__ == '__main__':
    instances = instSet_transform()
    instances.to_csv("./input/input_" + ".csv")
    scheduler = Scheduler(instances)
    i = 0
    for query in QUERIES:
        print(i)
        results = scheduler.calc_time(query)
        results = [results]
        results = calc_cost(results)
        results[0].to_csv("./output/output_" + str(i) + ".csv")
        best = results[0].iloc[0]
        scheduler.schedule(query, best)

        i += 1
