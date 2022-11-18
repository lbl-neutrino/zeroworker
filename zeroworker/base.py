#!/usr/bin/env python3

import time


class ListReaderBase:
    def __init__(self, timeout_mins=None):
        self._timeout_mins = timeout_mins

        if self._timeout_mins:
            self._tstart = time.time()

    def __iter__(self):
        return self

    def __next__(self):
        self._check_timeout()
        return self._do_next()

    def _do_next(self):
        raise NotImplementedError

    def _check_timeout(self):
        if self._timeout_mins:
            delta = (time.time() - self._tstart) / 60
            if delta > self._timeout_mins:
                print('Terminating due to specified timeout')
                raise StopIteration


class ListWriterBase:
    def __init__(self):
        self.__in_context = False

    def __enter__(self):
        self.__in_context = True
        return self

    def __exit__(self, *_exc):
        self.__in_context = False
        self.close()

    def close(self):
        pass

    def _do_put(self, line):
        raise NotImplementedError

    def put(self, line):
        if not self.__in_context:
            raise RuntimeError('ListWriter must be used in "with" block')
        self._do_put(line)

    # For convenience
    def log(self, line):
        tstamp = time.strftime('%Y-%m-%dT%H:%M:%S')
        self.put(f'{tstamp} {line}')
