#!/usr/bin/env python

import aio_pika
import logging
import logging.handlers
import asyncio
import argparse
import daemon
import uuid
import time
import subprocess
import threading
import sys
from redis import Redis

from pidlockfile import PIDLockFile
from setproctitle import setproctitle
from src import utils, registrations

log = logging.getLogger()
log.setLevel(logging.INFO)
syslog = logging.handlers.SysLogHandler(address="/dev/log")
syslog.ident = "trace_dispatcher.py: "
syslog.setLevel(logging.WARNING)
log.addHandler(syslog)
console = logging.StreamHandler()
console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
console.setLevel(logging.DEBUG)
log.addHandler(console)

rpc_xch = None
rpcs = dict()
rpc_id = None
trace_xch = None
channel = None

async def main():
    global rpc_xch
    global rpcs
    global rpc_id
    global redis
    global adsb_xch
    global trace_xch
    global channel
    global redlock

    rabbit = await aio_pika.connect_robust(args.rabbit)
    channel = await rabbit.channel()
    await channel.set_qos(prefetch_count=15)

    rpc_xch = await registrations.Exchanges.RPC(channel)
    rpc_queue = await channel.declare_queue(utils.random_string_with_prefix("trace_dispatcher_rpc_"), exclusive = True)
    rpc_id = rpc_queue.name
    await rpc_queue.bind(rpc_xch)

    trace_xch = await registrations.Exchanges.Traces(channel)
    adsb_xch = await registrations.Exchanges.ADSB(channel)
    adsb_queue = await channel.declare_queue("trace_dispatcher_adsb", exclusive=True)
    await adsb_queue.bind(adsb_xch, "#")

    redis = Redis.from_url(args.redis)

    try:
        await asyncio.gather(consume(rpc_queue, on_rpc_message, no_ack = True),
                             consume(adsb_queue, on_adsb_message))
    except asyncio.exceptions.CancelledError:
        pass
    finally:
        await rabbit.close()
        redis.close()

async def consume(queue, on_message, no_ack = False):
    async with queue.iterator(no_ack = no_ack) as q:
        async for message in q:
            if no_ack:
                await on_message(message)
            else:
                async with message.process():
                    await on_message(message)

async def on_rpc_message(message):
    if message.correlation_id in rpcs:
        # the completing future will delete its entry. or maybe it shouldn't.
        future = rpcs[message.correlation_id]
        future.set_result(message.body.decode())

async def on_adsb_message(message):
    icao = message.headers["icao"]
    lock_key = f"/katc/{icao}_trace"

    if redis.set(lock_key, "dispatch", nx=True, get=True) == None:
        trace_args = ["pipenv", "run", "./trace.py", "-i", icao, "-r", args.rabbit, "-s", args.redis, "-d"]
        log.info(f"Tracing {icao}")
        subprocess.run(trace_args)

async def call_rpc(method_name, args):
    call_id = str(uuid.uuid4())

    await rpc_xch.publish(aio_pika.Message(args.encode(),
                                           reply_to = rpc_id,
                                           correlation_id = call_id))
    rpcs[call_id] = future = asyncio.get_event_loop().create_future()

    try:
        result = await asyncio.wait_for(future, 2)
        del rpcs[call_id]
        return future.result()
    except TimeoutError:
        del rpcs[call_id]
        raise TimeoutError(f"Failed RPC {method_name}({args}): timed out ({call_id})")

if __name__ == "__main__":
    global args

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--daemon", action="store_true")
    parser.add_argument("-p", "--pidfile")
    parser.add_argument("-r", "--rabbit", required=True)
    parser.add_argument("-s", "--redis", required=True)
    args = parser.parse_args()

    if args.daemon:
        if args.pidfile is None or len(args.pidfile) == 0:
            log.error("-p/--pidfile is required when --daemon is present")
            sys.exit(1)

        with daemon.DaemonContext(pidfile=PIDLockFile(args.pidfile, timeout=2.0)):
            setproctitle("katc: trace_dispatcher.py")
            asyncio.run(main())
    else:
        asyncio.run(main())
