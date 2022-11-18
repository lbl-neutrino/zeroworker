#!/usr/bin/env python3

import zmq

from .base import ListReaderBase, ListWriterBase


READER_SOCK_NAME = 'reader.sock'
WRITER_SOCK_NAME = 'writer.sock'


class ZmqListReader(ListReaderBase):
    def __init__(self, sockdir, **kwargs):
        super().__init__(**kwargs)

        ctx = zmq.Context()
        self.sock = ctx.socket(zmq.REQ)
        self.sock.connect(f'ipc://{sockdir}/{READER_SOCK_NAME}')

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
        self.sock.connect(f'ipc://{sockdir}/{WRITER_SOCK_NAME}')

    def _do_put(self, line):
        self.sock.send_string(line)
