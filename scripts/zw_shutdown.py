#!/usr/bin/env python3

import argparse
import os
import time

import zmq

from zeroworker.zmq_io import READER_SOCK_NAME, WRITER_SOCK_NAME


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('sockdir')
    args = ap.parse_args()

    ctx = zmq.Context()

    def send_quit(name, socktype):
        sockpath = f'{args.sockdir}/{name}'
        sock = ctx.socket(socktype)
        sock.connect(f'ipc://{sockpath}')
        sock.send_string('QUIT')

        while os.path.exists(sockpath):
            time.sleep(0.5)

    send_quit(READER_SOCK_NAME, zmq.REQ)
    send_quit(WRITER_SOCK_NAME, zmq.PUSH)

    print("Party's over")


if __name__ == '__main__':
    main()
