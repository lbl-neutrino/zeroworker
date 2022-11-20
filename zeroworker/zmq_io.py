#!/usr/bin/env python3

from pathlib import Path
import time

import zmq

from .base import ListReaderBase, ListWriterBase


READER_SOCK_NAME = 'reader.sock'
WRITER_SOCK_NAME = 'writer.sock'


class ZmqListReader(ListReaderBase):
    def __init__(self, sockdir, **kwargs):
        super().__init__(**kwargs)

        ctx = zmq.Context()
        self.sock = ctx.socket(zmq.REQ)
        sockpath = Path(sockdir).joinpath(READER_SOCK_NAME)
        while not sockpath.exists():
            time.sleep(5)
        self.sock.connect(f'ipc://{sockpath}')

    def __next__(self):
        self._check_timeout()
        self.sock.send_string('')
        item = self.sock.recv_string()
        if item == '':
            raise StopIteration
        return item


class ZmqListWriter(ListWriterBase):
    def __init__(self, sockdir, **kwargs):
        super().__init__(**kwargs)

        ctx = zmq.Context()
        self.sock = ctx.socket(zmq.PUSH)
        sockpath = Path(sockdir).joinpath(WRITER_SOCK_NAME)
        while not sockpath.exists():
            time.sleep(5)
        self.sock.connect(f'ipc://{sockpath}')

    def _do_put(self, line):
        self.sock.send_string(line)
