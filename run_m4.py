from models.utils import calc_cost
from preprocessing.instances import inst_set_transform
from models.scripts.m4 import calc_time_m4


if __name__ == '__main__':
    instances = inst_set_transform()
    instances.to_csv("./input/input_" + ".csv")
    instances = calc_time_m4(instances)
    instances = calc_cost(instances)
    index = 1
    for i in instances:
        i.to_csv("./output/output_" + str(index) + ".csv")
        index += 1
