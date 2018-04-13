import sys
import time


class copyConsoleToFile(object):
    """ Enables logging of console output to a file, use
    >> tlogger = copyConsoleToFile('logfile.txt', 'w')
    at the start of the code to start logging.
    """
    def __init__(self, name, mode, add_timestamp = False):
        self.file = open(name, mode)
        self.stdout = sys.stdout
        self.add_timestamp = add_timestamp
        sys.stdout = self

    def close(self):
        if self.stdout is not None:
            sys.stdout = self.stdout
            self.stdout = None
        if self.file is not None:
            self.file.close()
            self.file = None

    def write(self, data):
        if self.add_timestamp:
            self.file.write(time.strftime("%Y-%m-%d %H:%M:%S") + ': ')
        self.file.write(data)
        self.stdout.write(data)

    def flush(self):
        self.file.flush()
        self.stdout.flush()

    def __del__(self):
        self.close()