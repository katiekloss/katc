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

from pidlockfile import PIDLockFile
from setproctitle import setproctitle
from src import utils, registrations

log = logging.getLogger()
log.setLevel(logging.INFO)
syslog = logging.handlers.SysLogHandler(address="/dev/log")
syslog.ident = "flight_state_tracker.py: "
log.addHandler(syslog)
console = logging.StreamHandler()
console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
console.setLevel(logging.DEBUG)
log.addHandler(console)

rpc_xch = None
rpcs = dict()
rpc_id = None

flight_state_xch = None
flight_state_last_seen = dict()
flyby_traces_xch = None
trace_needed_count = dict()

async def main():
    global flight_state_xch
    global rpc_xch
    global rpcs
    global rpc_id
    global flyby_traces_xch

    rabbit = await aio_pika.connect_robust(args.rabbit)
    channel = await rabbit.channel()
    xch = await channel.get_exchange("adsb")
    adsb_queue = await channel.declare_queue("", durable=True, exclusive=True)
    await adsb_queue.bind(xch, "#")

    flight_state_xch = await channel.declare_exchange("flight_state_changed", aio_pika.ExchangeType.FANOUT, durable = True)
    flight_state_stream = await channel.declare_queue("flight_state_changes",
                                                      durable = True,
                                                      arguments={"x-max-age": "7D",
                                                                 "x-queue-type": "stream"})
    await flight_state_stream.bind(flight_state_xch)

    rpc_xch = await channel.declare_exchange("rpc", aio_pika.ExchangeType.DIRECT, durable = True)
    rpc_queue = await channel.declare_queue("", exclusive = True)
    rpc_id = rpc_queue.name

    await rpc_queue.bind(rpc_xch)

    flyby_traces_unrouted_xch = await registrations.Exchanges.FlybyTracesUnrouted(channel)
    flyby_traces_unrouted_queue = await channel.declare_queue(utils.random_string_with_prefix("flight_state_tracker_unrouted_"), exclusive = True)
    await flyby_traces_unrouted_queue.bind(flyby_traces_unrouted_xch)

    flyby_traces_xch = await registrations.Exchanges.FlybyTraces(channel)

    await asyncio.gather(consume(adsb_queue, on_adsb_message),
                         consume(rpc_queue, on_rpc_message, no_ack = True),
                         consume(flyby_traces_unrouted_queue, on_unrouted_message),
                         session_cleaner())

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

    if icao not in flight_state_last_seen:
        await flight_state_xch.publish(aio_pika.Message("{}".encode(),
                                                        headers={"icao": icao}),
                                       "start")
        log.info(f"Started session for {icao}")

    flight_state_last_seen[icao] = time.time()

async def on_rpc_message(message):
    if message.correlation_id in rpcs:
        # the completing future will delete its entry. or maybe it shouldn't.
        future = rpcs[message.correlation_id]
        future.set_result(message.body.decode())

async def on_unrouted_message(message):
    icao = message.headers["icao"]
    log.info(f"Need to start trace for {icao}")
    subprocess.run(["pipenv", "run", "./flyby_tracer.py", "-i", icao, "-r", args.rabbit, "-d"])

    # await asyncio.sleep(2)
    # await flyby_traces_xch.publish(message, icao)

    #if icao in trace_needed_count:
    #    trace_needed_count[icao] += 1
    #    log.warning(f"{icao} has needed a trace {trace_needed_count[icao]} time(s)")
    #else:
    #    trace_needed_count[icao] = 1


async def session_cleaner():
    while True:
        await asyncio.sleep(1)
        now = time.time()
        to_delete = list()
        for icao in flight_state_last_seen.keys():
            if now - flight_state_last_seen[icao] > 45:
                await flight_state_xch.publish(aio_pika.Message("{}".encode(),
                                                                headers={"icao":icao}),
                                               "finish")
                to_delete.append(icao)
                log.info(f"Ended session for {icao}")

        for icao in to_delete:
            del flight_state_last_seen[icao]

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
    args = parser.parse_args()

    if args.daemon:
        if args.pidfile is None or len(args.pidfile) == 0:
            log.error("-p/--pidfile is required when --daemon is present")
            sys.exit(1)

        with daemon.DaemonContext(pidfile=PIDLockFile(args.pidfile, timeout=2.0)):
            setproctitle("katc: flight_state_tracker.py")
            asyncio.run(main())
    else:
        asyncio.run(main())
