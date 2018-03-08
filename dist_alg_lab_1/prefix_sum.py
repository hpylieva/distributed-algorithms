import math
import multiprocessing
import time
import matplotlib.pyplot as plt

def sequential_prefix_sum(sequence):
    """
    Implement a basic sequential prefix sum
    :param sequece: list
    :return: list
    """
    prefix_sum = [0] * (len(sequence) + 1)
    for i in range(1, len(sequence) + 1):
        prefix_sum[i] = sequence[i - 1] + prefix_sum[i - 1]
    return prefix_sum


def _upward_summator(array, level, num, proc, print_log):
    """
    Performs upward summation for parallel prefix sum
    :param level:
    :param num: number of core
    :param proc: number of processor
    """
    lbound = proc * num
    rbound = proc * (num + 1)
    if print_log:
        print('Upper Worker ' + str(num) + ' ' + str(lbound) + ' ' + str(rbound))
    for sum_index in range(lbound + 2 ** (level + 1) - 1, rbound, 2 ** (level + 1)):
        array[sum_index] = array[sum_index] + array[sum_index - 2 ** level]


def _downward_summator(array, level, num, proc, print_log):
    """
    Performs downward summation for parallel prefix sum
    :param level:
    :param num: number of core
    :param proc: number of processor
    """
    lbound = proc * num
    rbound = proc * (num + 1)
    if (print_log):
        print('Down Worker ' + str(num) + ' ' + str(lbound) + ' ' + str(rbound))
    for sum_index in range(lbound + 2 ** (level + 1) - 1, rbound, 2 ** (level + 1)):
        val = array[sum_index]
        array[sum_index] = array[sum_index] + array[sum_index - 2 ** level]
        array[sum_index - 2 ** level] = val


def parallel_prefix_sum(sequence, max_cores=4, print_log = False):
    """
    Implements algorithm based on the one presented by Blelloch (1990)
    :param sequnece: input list of values
    :return: list containing prefix sum
    """
    length = len(sequence)
    input_seq = sequence.copy()
    input_seq += [0]
    depth = math.log(length, 2)
    if print_log:
        print("Depth:", depth)

    array = multiprocessing.Array('i', input_seq)
    jobs = []

    # first_phase - upward summator
    for level in range(0, int(depth)):
        if print_log:
            print("Level:", level)
        processing_field = 2 ** (level + 1)
        core_number = int(length / processing_field)
        if core_number > max_cores:
            core_number = max_cores
            processing_field = int(length / core_number)
        for i in range(core_number):
            p = multiprocessing.Process(target = _upward_summator, args=(array, level, i, processing_field, print_log))
            p.daemon = False
            jobs.append(p)
            p.start()
        for p in jobs:
            p.join()
        if print_log:
            print(array[:])

    cumsum = array[length - 1]
    array[length - 1] = 0

    # second phase - downward summator
    for level in range(int(depth), -1, -1):
        if print_log:
            print("Level:", level)
        processing_field = 2 ** (level + 1)
        core_number = int(length / processing_field)
        if core_number > max_cores:
            core_number = max_cores
            processing_field = int(length / core_number)
        for i in range(core_number):
            p = multiprocessing.Process(target = _downward_summator, args=(array, level, i, processing_field, print_log))
            p.daemon = False
            jobs.append(p)
            p.start()
        for p in jobs:
            p.join()
        array[length] = cumsum
        if (print_log):
            print(array[:])
    return array


if __name__ == '__main__':
    max_cores = 4
    #sequence_size = 200
    print_log = False
    run_single = True

    if run_single:
        sequence = [1]*16
        parallel_prefix_sum(sequence, max_cores, True)
    else:
        sizes = range(1, 15)
        time_sequential = []
        time_parallel = []

        for size in sizes:
            sequence_size = 2**size
            sequence = [1] * sequence_size

            time_start = time.time()
            sequential_prefix_sum(sequence)
            time_taken = time.time() - time_start
            time_sequential.append(time_taken)
            print('Sequential prefix sum with input sequence length {0} took {1}s'.format(sequence_size, time_taken))

            time_start = time.time()
            parallel_prefix_sum(sequence, max_cores)
            time_taken = time.time() - time_start
            time_parallel.append(time_taken)

            print('Parallel prefix sum with input sequence length {0} took {1}s'.format(sequence_size, time_taken))

        plt.figure()
        plt.plot(sizes, time_sequential)
        plt.plot(sizes, time_parallel)
        plt.legend(["Sequential", "Parallel"], loc=2)
        plt.xlabel("Size of input sequence")
        plt.ylabel("Run time")
        plt.show()

        plt.savefig('result.png')