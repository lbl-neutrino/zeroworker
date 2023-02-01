#!/usr/bin/env python3

import argparse
from multiprocessing import Process
import os
import time

import zmq

from zeroworker import ListReaderBase, ListWriterBase
from zeroworker import LockfileListReader, LockfileListWriter
from zeroworker.zmq_io import READER_SOCK_NAME, WRITER_SOCK_NAME

# When using --immortal, how long to wait between attempts to pull new input
# after draining it
POLL_INTERVAL_SEC = 30

class InputBuffer:
    def __init__(self, sockdir, infile, chunksize, timeout_mins=None, immortal=False):
        ctx = zmq.Context()
        self.sockpath = f'{sockdir}/{READER_SOCK_NAME}'
        self.sock = ctx.socket(zmq.REP)
        self.sock.bind(f'ipc://{self.sockpath}')

        self.reader = LockfileListReader(infile, chunksize=chunksize, timeout_mins=timeout_mins)
        self.done = False       # to avoid grabbing the lock when we know it's all over

        self.immortal = immortal

    def serve(self):
        while True:
            msg = self.sock.recv_string()

            if msg == 'QUIT':
                break

            item = ''

            while True:
                try:
                    item = next(self.reader)
                    break
                except StopIteration: # timed out or drained input
                    if self.immortal:
                        time.sleep(POLL_INTERVAL_SEC)
                    else:
                        self.done = True
                        break

            self.sock.send_string(item)

        os.remove(self.sockpath)


class OutputBuffer:
    def __init__(self, sockdir, outfile, chunksize):
        ctx = zmq.Context()
        self.sockpath = f'{sockdir}/{WRITER_SOCK_NAME}'
        self.sock = ctx.socket(zmq.PULL)
        self.sock.bind(f'ipc://{self.sockpath}')

        self.writer = LockfileListWriter(outfile, chunksize=chunksize)

    def serve(self):
        with self.writer:  # ensure cache is flushed
            while True:
                item = self.sock.recv_string()

                if item == 'QUIT':
                    break

                self.writer.put(item)

        os.remove(self.sockpath)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('sockdir')
    ap.add_argument('infile')
    ap.add_argument('-I', '--input-chunksize', type=int, default=32)
    ap.add_argument('-O', '--output-chunksize', type=int, default=1)
    ap.add_argument('-t', '--timeout', type=float, help='minutes')
    ap.add_argument('--immortal', action='store_true',
                    help='Stick around and wait for new input after draining it')
    args = ap.parse_args()

    def serve_inbuf():
        ib = InputBuffer(args.sockdir, args.infile, args.input_chunksize,
                         timeout_mins=args.timeout, immortal=args.immortal)
        ib.serve()

    def serve_outbuf():
        donefile = args.infile + '.done'
        ob = OutputBuffer(args.sockdir, donefile, args.output_chunksize)
        ob.serve()

    Process(target=serve_inbuf).start()
    Process(target=serve_outbuf).start()


if __name__ == '__main__':
    main()
