import bisect
import math
import multiprocessing
import random
import sys
import time
import matplotlib.pyplot as plt

from console_output_to_file import copyConsoleToFile


def merge(*args):
    """
    Implements merge of 2 sorted lists
    :param args: support explicit left/right args, as well as a two-item
                tuple which works more cleanly with multiprocessing.
    :return: merged list
    """
    left, right = args[0] if len(args) == 1 else args
    left_length, right_length = len(left), len(right)
    left_index, right_index = 0, 0
    merged = []
    while left_index < left_length and right_index < right_length:
        if left[left_index] <= right[right_index]:
            merged.append(left[left_index])
            left_index += 1
        else:
            merged.append(right[right_index])
            right_index += 1
    if left_index == left_length:
        merged.extend(right[right_index:])
    else:
        merged.extend(left[left_index:])
    return merged


def sequential_merge_sort(data, verbose = False):
    length = len(data)
    if length <= 1:
        return data
    middle = length // 2
    left = sequential_merge_sort(data[:middle])
    right = sequential_merge_sort(data[middle:])
    if verbose:
        print(left)
        print(right)
    return merge(left, right)


def parallel_naive_merge_sort(data, verbose=False):
    # Creates a pool of worker processes, one per CPU core.
    # We then split the initial data into partitions, sized
    # equally per worker, and perform a regular merge sort
    # across each partition.
    workers_count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=workers_count)
    size = int(math.ceil(float(len(data)) / workers_count))
    data = [data[i * size:(i + 1) * size] for i in range(workers_count)]
    data = pool.map(sequential_merge_sort, data)
    if verbose:
        print("Each partition is now sorted.\n", data)
        print("Starting partitions' merge phase.\n")
    # Each partition is now sorted - we now just merge pairs of these
    # together using the worker pool, until the partitions are reduced
    # down to a single sorted result.
    while len(data) > 1:
        data = [(data[i], data[i + 1]) for i in range(0, len(data), 2)]
        data = pool.map(merge, data)
    return data[0]


def _parallel_merger(shared_array, level, worker_number, proc_field_size, verbose=False):
    """
    Facilitator of parallel merge sort
    :param shared_array: array in shared memory of workers
    :param level: level
    :param worker_number: number of the worker
    :param proc_field_size: the size of field processed by each worker on current step
    """
    lbound = proc_field_size * worker_number
    rbound = proc_field_size * (worker_number + 1)
    if verbose:
        # prints number of worker and the range of indices it processes
        print('Worker_{0}: indices {1} to {2} '.format(worker_number, lbound,rbound))
    for i in range(lbound + 2 ** level, rbound, 2 ** (level + 1)):
        # left list: [i - 2**level, i-1]
        # right list: [i, i + 2**level - 1]
        shared_array[i - 2 ** level: i + 2 ** level] = _merge_using_binary_search(shared_array[i - 2 ** level: i],
                                                                                  shared_array[i: i + 2 ** level])


def _merge_using_binary_search(l, r):
    """
    Performs merge using binary search to find position of
    element in result array
    :param l: left list to merge
    :param r: right list to merge
    :return: merged list of size len(l)+len(r)==2*len(l)
    """

    merged_list = []
    last_taken_r_position = 0

    for e_l in l:
        position_in_r = bisect.bisect_right(r,e_l)
        if position_in_r>0:
            merged_list.extend(r[last_taken_r_position:position_in_r])
        merged_list.append(e_l)
        last_taken_r_position = position_in_r
    if len(merged_list)<(len(l)+len(r)):
        merged_list.extend(r[len(merged_list)-len(l):])
    return merged_list


def parallel_merge_sort(input, verbose=False):
    """
    Performs parallel merge sort multiple processes
    :param input: list of input elements
    :return: sorted list
    """
    max_cores = multiprocessing.cpu_count()
    jobs = []
    length = len(input)
    shared_array = multiprocessing.Array('i', input)
    depth = math.log(length, 2)
    for level in range(0, int(depth)):
        if verbose:
            print("Level:", level)
        processing_field_size = 2 ** (level + 1)
        core_number = int(length / processing_field_size)
        if core_number > max_cores:
            core_number = max_cores
            processing_field_size = int(length / core_number)
        for i in range(core_number):
            p = multiprocessing.Process(target=_parallel_merger, args=(shared_array, level, i, processing_field_size, verbose))
            p.daemon = False
            jobs.append(p)
            p.start()
        for p in jobs:
            p.join()
        if (verbose):
            print("Current state of array:")
            print(shared_array[:])
    return shared_array[:]


if __name__ == "__main__":
    random.seed(128)

    run_single_time =  not True

    if run_single_time:
        tlogger = copyConsoleToFile('compare_one_run_log.txt', 'w')
        power = 5
        size = 2 ** power
        gen_data_margin = size
        data_unsorted = random.sample(range(size), size)
        print("Generated unsorted data:\n", data_unsorted)
        for sort in sequential_merge_sort, parallel_naive_merge_sort, parallel_merge_sort:
            start = time.time()
            data_sorted = sort(data_unsorted, True)
            print(data_sorted)
            time_taken = time.time() - start
            print(sort.__name__, time_taken, sorted(data_unsorted) == data_sorted)

    else:
        tlogger = copyConsoleToFile('compare_on_list_size_log.txt', 'w')
        powers = range(1, 15)
        sizes = [2 ** p for p in powers]
        time_sequential = []
        time_naive_parallel = []
        time_parallel = []
        execution_time = [[],[],[]]

        for size in sizes:
            data_unsorted = random.sample(range(size), size)
            for i, sort in enumerate((sequential_merge_sort, parallel_naive_merge_sort, parallel_merge_sort)):
                start = time.time()
                data_sorted = sort(data_unsorted)
                time_taken = time.time() - start
                execution_time[i].append(time_taken)
                print(sort.__name__+' with input sequence length {0} took {1}s'.format(size, time_taken))

        plt.figure()
        plt.plot(powers, execution_time[0])
        plt.plot(powers, execution_time[1])
        plt.plot(powers, execution_time[2])
        plt.legend(["Sequential", "Naive parallel", "Parallel"], loc=2)
        plt.xlabel("Input size: 2^x")
        plt.ylabel("Execution time")
        plt.savefig('result.png')
        plt.show()

