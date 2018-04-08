import os
import sys
import time

from numpy import random

from parallel_mapreduce import MapReduce

if __name__ == '__main__':
    random.seed(129)
    population = range(10)
    power = 10
    print_input = False
    num_workers = 4

    input_digits = list(random.choice(population, 2**power))

    if print_input:
        print("input digits \n", input_digits)
    else:
        # save to a text file
        with open(os.path.dirname(sys.argv[0])+'\\input.txt', 'w') as f:
            f.write(' '.join(str(e) for e in input_digits))

    map_reduce_job = MapReduce()

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
        digit_sum+=count

    print('Total sum of digit counts: ', digit_sum)
    print('\nTime spent: {} s'.format(end-start))