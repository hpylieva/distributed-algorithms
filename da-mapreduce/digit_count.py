import os
import sys
import time

from utils.format_output import wrap_list
from numpy import random

from parallel_mapreduce import MapReduce
from utils.console_output_to_file import copyConsoleToFile


def digit_count_mapper(input_el, output):
    output[input_el] += [1]


def digit_count_reducer(input_key, input_value, output):
    output[input_key] += sum(input_value)


if __name__ == '__main__':
    random.seed(129)
    population = range(10)
    power = 10
    print_input = False
    num_workers = 4
    tlogger = copyConsoleToFile('logfile.txt', 'w')
    input_digits = list(random.choice(population, 2 ** power))

    if print_input:
        print("Input digits")
        wrap_list(input_digits)
        # print("input digits \n", input_digits)
    # save to a text file
    with open(os.path.dirname(sys.argv[0]) + '\\input.txt', 'w') as f:
        # f.write(' '.join(str(e) for e in input_digits))
        f.write("Input digits\n"+wrap_list(input_digits))

    map_reduce_job = MapReduce(digit_count_mapper, digit_count_reducer)

    start = time.time()
    digit_counts = map_reduce_job(input_digits, num_workers, True)
    end = time.time()

    digit_sum = 0
    print('\nResult of count digits with mapreduce\n')
    for digit, count in digit_counts:
        print('{digit:}: {count:5}'.format(
            digit=digit,
            count=count)
        )
        digit_sum += count

    print('Total sum of digit counts: ', digit_sum)
    print('\nTime spent: {} s'.format(end - start))
