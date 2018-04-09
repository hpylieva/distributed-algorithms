import collections
import math
import multiprocessing
import time


class MapReduce(object):
    def __init__(self, the_mapper, the_reducer):
        """
        :param the_mapper: the mapper specified by user of the class
        :param the_reducer: the reducer specified by user of the class
        """
        self.the_mapper = the_mapper
        self.the_reducer = the_reducer

    def _mapper(self, input, output, length, worker_number, chunksize):
        print("Thread {0} is working".format(worker_number))
        id_start = worker_number * chunksize
        id_end = min((worker_number + 1) * chunksize, length)
        print("It processes indexes of input array between {0} and {1}".format(id_start, id_end))
        for i in range(length)[id_start:id_end]:
            if input[i] in output:
                self.the_mapper(input[i],output)
            else:
                output[input[i]] = []
                self.the_mapper(input[i], output)

    def map_parallel(self, input, num_threads=4, verbose=False):
        """
        A standard map part of MapReduce job. The work is done in num_workers threads
        as much in parallel as Python multiprocessing allows.
        :param input: list of values
        :param num_threads: number of threads to handle work
        :return: result of applying _mapper in form of (key, value)
        """
        input_len = len(input)
        input_dict = multiprocessing.Manager().dict(enumerate(input))
        output_dict = multiprocessing.Manager().dict()
        chunksize = math.ceil(input_len / num_threads)

        # Create the threads.
        threads = []
        # Start the threads.
        for i in range(num_threads):
            t = multiprocessing.Process(target=self._mapper, args=(input_dict, output_dict, input_len, i, chunksize))
            threads.append(t)
            t.start()
            # I've introduced delay as otherwise my processed didn't want to wait for each other correctly
            # as a result not all the values were processed
            time.sleep(1)
            if verbose:
                print("Current state of output array:")
                print("{" + "\n".join("{}: {}".format(k, v) for k, v in output_dict.items()) + "}")
        # Wait for the threads to finish.
        for t in threads:
            t.join()
        for t in threads:
            t.terminate()
        return output_dict

    def _reducer(self, input, output, input_keys, worker_number, chunksize):
        print("Thread {0} is working".format(worker_number))
        id_start = worker_number * chunksize
        id_end = min((worker_number + 1) * chunksize, len(input_keys))
        print("It processes keys: ", input_keys[id_start: id_end])
        for key in input_keys[id_start:id_end]:
            if key in output:
                self.the_reducer(key, input[key], output)
            else:
                output[key] = 0
                self.the_reducer(key, input[key], output)

    def reduce_parallel(self, input_dict, num_threads=4, verbose=False):
        """
        A standard reduce part of MapReduce job. The work is done in num_workers threads
        as much in parallel as Python multiprocessing allows.
        :param input_dict: result of map
        :param num_threads: number of threads to handle work
        :return: result of applying _reducer in form of (key, value)
        """
        input_keys = [*input_dict.keys()]
        input_dict = multiprocessing.Manager().dict(input_dict)
        output_dict = multiprocessing.Manager().dict()
        chunksize = math.ceil(len(input_keys) / num_threads)
        # Create the threads.
        threads = []
        # Start the threads.
        for i in range(num_threads):
            t = multiprocessing.Process(target=self._reducer, args=(input_dict, output_dict, input_keys, i, chunksize))
            threads.append(t)
            t.start()
            # I've introduced delay as otherwise my processed didn't want to wait for each other correctly
            # as a result not all the values were processed
            time.sleep(1)
            if verbose:
                print("Current state of output array:")
                print("{" + "\n".join("{}: {}".format(k, v) for k, v in output_dict.items()) + "}")
        # Wait for the threads to finish.
        for t in threads:
            t.join()
        for t in threads:
            t.terminate()
        ordered_tuple = collections.OrderedDict(sorted(output_dict.items()))
        return ordered_tuple.items()

    def __call__(self, inputs, num_workers, verbose=False):
        """
        Processes the inputs through the map and reduce functions given.
        :param inputs: an iterable containing the input data to be processed.
        :param num_workers: number of threads to handle both map and reduce specified by user
        :param verbose: allows to restrict verbosity of the algorithm, if False - less is printed in logs
        :return: reduced values: result of MapReduce job
        """
        print("Map is running...")
        map_responses = self.map_parallel(inputs, num_workers, verbose)
        if verbose:
            print("Map response")
            print("{" + "\n".join("{}: {}".format(k, v) for k, v in map_responses.items()) + "}")
        print("Map is finished.")
        print("Reduce is running...")
        reduced_values = self.reduce_parallel(map_responses, num_workers, verbose)
        print("Reduce is finished.")
        return reduced_values
