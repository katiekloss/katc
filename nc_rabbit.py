#!/usr/bin/env python
import argparse
import sys
import aio_pika
import aio_pika.abc
import asyncio
import time
import os
import daemon

from pidlockfile import PIDLockFile
from setproctitle import setproctitle

async def dump1090_loop(args) -> None:
    conn_string = args.rabbit

    rabbit: aio_pika.RobustConnection = await aio_pika.connect_robust(conn_string)
    channel: aio_pika.abc.AbstractChannel = await rabbit.channel()
    exchange = await channel.declare_exchange("mode_s", type = aio_pika.ExchangeType.DIRECT)

    reader, writer = await asyncio.open_connection(args.target_ip, 30002)
    
    async for line in reader:
        now = time.time_ns()
        line = line.rstrip()[1:-1]
                
        await exchange.publish(aio_pika.Message(body = line,
                                                timestamp = time.time()),
                                                routing_key = 'raw')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--daemon", action="store_true")
    parser.add_argument("-p", "--pidfile")
    parser.add_argument("-r", "--rabbit", required=True)
    parser.add_argument("-t", "--target-ip", required=True)
    args = parser.parse_args()

    if args.daemon:
        if args.pidfile is None or len(args.pidfile) == 0:
            print("-p/--pidfile is required when --daemon is present")
            sys.exit(1)

        with daemon.DaemonContext(pidfile=PIDLockFile(args.pidfile, timeout=2.0)):
            setproctitle("yetanother1090monitor: nc_rabbit")
            asyncio.run(dump1090_loop(args))
    else:
        asyncio.run(dump1090_loop(args))
