#!/usr/bin/env python

import aio_pika
import logging
import logging.handlers
import asyncio
import argparse
import daemon
import uuid

from pidlockfile import PIDLockFile
from setproctitle import setproctitle

log = logging.getLogger()
log.setLevel(logging.INFO)
syslog = logging.handlers.SysLogHandler(address="/dev/log")
syslog.ident = "flight_state_tracker.py: "
log.addHandler(syslog)
log.addHandler(logging.StreamHandler())

rpc_xch = None
rpcs = dict()
rpc_id = None

flight_state_xch = None
flight_state_last_seen = dict()

async def main(args):
    global flight_state_xch
    global rpc_xch
    global rpcs
    global rpc_id

    rabbit = await aio_pika.connect_robust(args.rabbit)
    channel = await rabbit.channel()
    xch = await channel.get_exchange("adsb")
    adsb_queue = await channel.declare_queue("", durable=True, exclusive=True)
    await adsb_queue.bind(xch, "#")

    flight_state_xch = await channel.declare_exchange("flight_state_changed", aio_pika.ExchangeType.FANOUT, durable = True)

    rpc_xch = await channel.declare_exchange("rpc", aio_pika.ExchangeType.DIRECT, durable = True)
    rpc_queue = await channel.declare_queue("", exclusive = True)
    rpc_id = rpc_queue.name

    await rpc_queue.bind(rpc_xch)

    await asyncio.gather(consume(adsb_queue, on_adsb_message),
                         consume(rpc_queue, on_rpc_message, no_ack = True))

async def consume(queue, on_message, no_ack = False):
    async with queue.iterator(no_ack = no_ack) as q:
        async for message in q:
            if no_ack:
                await on_message(message)
            else:
                async with message.process():
                    await on_message(message)

async def on_adsb_message(message):
    icao = message.headers["icao"]
    data = message.body.decode()

async def on_rpc_message(message):
    if message.correlation_id in rpcs:
        # the completing future will delete its entry. or maybe it shouldn't.
        future = rpcs[message.correlation_id]
        future.set_result(message.body.decode())

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
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--daemon", action="store_true")
    parser.add_argument("-p", "--pidfile")
    parser.add_argument("-r", "--rabbit", required=True)
    args = parser.parse_args()

    if args.daemon:
        if args.pidfile is None or len(args.pidfile) == 0:
            log.error("-p/--pidfile is required when --daemon is present")
            sys.exit(1)

        with daemon.DaemonContext(pidfile=PIDLockFile(args.pidfile, timeout=2.0)):
            setproctitle("katc: flight_state_tracker.py")
            asyncio.run(main(args))
    else:
        asyncio.run(main(args))
