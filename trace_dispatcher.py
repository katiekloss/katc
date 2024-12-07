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

from pidlockfile import PIDLockFile
from setproctitle import setproctitle
from src import utils, registrations
from aio_pika.exceptions import DeliveryError
from redis import asyncio as aioredis

log = logging.getLogger()
log.setLevel(logging.INFO)
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
    global adsb_xch
    global trace_xch
    global channel
    global redis

    redis = aioredis.Redis.from_url(args.redis)

    rabbit = await aio_pika.connect_robust(args.rabbit)
    channel = await rabbit.channel()
    adsb_channel = await rabbit.channel()

    rpc_xch = await registrations.Exchanges.RPC(channel)
    rpc_queue = await channel.declare_queue(utils.random_string_with_prefix("trace_dispatcher_rpc_"), exclusive = True)
    rpc_id = rpc_queue.name
    await rpc_queue.bind(rpc_xch)

    trace_xch = await registrations.Exchanges.TraceDispatch(adsb_channel)

    adsb_xch = await registrations.Exchanges.ADSB(adsb_channel)
    adsb_queue = await adsb_channel.declare_queue("trace_dispatcher_adsb", durable=True)
    await adsb_queue.bind(adsb_xch, "#")

    try:
        await asyncio.gather(consume(rpc_queue, on_rpc_message, no_ack = True),
                             consume(adsb_queue, on_adsb_message))
    except asyncio.exceptions.CancelledError:
        pass
    finally:
        await rabbit.close()

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
    if await redis.exists(f"trace_requested_{icao}", f"trace_{icao}") > 0:
        return

    request = await redis.set(f"trace_requested_{icao}", "1", nx=True)
    if not request:
        log.info(f"Another process requested {icao} first, ignoring: {request}")
        return

    await redis.lpush("trace_requests", icao)
    log.info(f"Requested {icao}")

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
