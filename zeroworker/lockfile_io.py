#!/usr/bin/env python3

import os
import sys
import time

from .base import ListReaderBase, ListWriterBase


class LockfileListReader(ListReaderBase):
    def __init__(self, filename, chunksize=1, **kwargs):
        super().__init__(**kwargs)

        self._filename = filename
        self._chunksize = chunksize

        self._all = []
        self._working = []
        self._lastmtime = 0

        self._load()

    def _load(self):
        self._all = open(self._filename).readlines()
        self._lastmtime = os.path.getmtime(self._filename)

    @property
    def _modified(self):
        return os.path.getmtime(self._filename) > self._lastmtime

    @property
    def _lock_file(self):
        return self._filename + '.lock'

    @property
    def _offset_file(self):
        return self._filename + '.offset'

    def _do_next(self):
        if not self._working:
            self._pull()
            if not self._working:
                raise StopIteration
        return self._working.pop(0).strip()

    def _read_offset(self):
        try:
            return int(open(self._offset_file).read())
        except FileNotFoundError:
            return 0
        except ValueError:
            print('WARNING: Invalid/empty offset file. This should not happen',
                  file=sys.stderr)
            time.sleep(60)
            return self._read_offset()

    def _write_offset(self, offset):
        with open(self._offset_file, 'w') as f:
            f.write(f'{offset}\n')

    def _pull(self):
        self._check_timeout()

        # NOTE: -r -1 essentially means "retry forever" with delays starting at
        # 5sec and increasing to 60sec
        os.system(f'time dotlockfile -r -1 {self._lock_file}' )

        if self._modified:
            self._load()

        offset = self._read_offset()

        if offset < len(self._all):
            self._write_offset(min(len(self._all), offset + self._chunksize))

        os.system(f'dotlockfile -u {self._lock_file}')

        # Slicing past the end of the list is harmless
        self._working = self._all[offset : offset + self._chunksize]


class LockfileListWriter(ListWriterBase):
    def __init__(self, filename, chunksize=1, **kwargs):
        super().__init__(**kwargs)

        self._filename = filename
        self._chunksize = chunksize

        self._buf = []

    @property
    def _lock_file(self):
        return self._filename + '.lock'

    def _do_put(self, line):
        self._buf.append(line)
        if len(self._buf) == self._chunksize:
            self._flush()

    def close(self):
        if self._buf:
            self._flush()

    def _flush(self):
        chunk = '\n'.join(self._buf) + '\n'
        os.system(f'dotlockfile -r -1 {self._lock_file}')
        with open(self._filename, 'a') as f:
            f.write(chunk)
        os.system(f'rm -f {self._lock_file}')
        self._buf = []
