#multiprocessing_wordcount.py
import multiprocessing
import string

from parallel_mapreduce import MapReduce

def map_digits(input):
    # output = []
    # for e in input:
    #     output.append((e, 1))
    # return output
        #zip(input, 1*[len(input)])
    return (input, 1)

def count_digits(item):
    """Convert the partitioned data for a digit to a
    tuple containing the word and the number of occurences.
    """
    digit, occurences = item
    return (digit, sum(o for o in occurences))


if __name__ == '__main__':
    from numpy import random
    import operator
    import os, sys
    import time

    random.seed(129)

    population = range(10)
    power = 10 #10
    print_input = False
    num_workers = 4

    input_digits = list(random.choice(population, 2**power, num_workers))

    if print_input:
        print("input digits \n", input_digits)
    else:
        # save to a text file
        with open(os.path.dirname(sys.argv[0])+'\\input.txt', 'w') as f:
            f.write(' '.join(str(e) for e in input_digits))

    mapper = MapReduce(map_digits, count_digits, num_workers)
    chunksize = 4

    start = time.time()
    digit_counts = mapper(input_digits, chunksize)
    end = time.time()

    print('\nResult of count digits with mapreduce\n')
    for digit, count in digit_counts:
        print('{digit:}: {count:5}'.format(
            digit=digit,
            count=count)
        )
    print('\nTime spent: {} s'.format(end-start))