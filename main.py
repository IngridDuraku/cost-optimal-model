from preprocessing.instances import instSet_transform
from models.scripts.m2 import calc_time


if __name__ == '__main__':
    instances = instSet_transform()
    instances = calc_time(instances)
    print(instances)
