#!/usr/bin/env python
import aio_pika
import argparse
import asyncio
import time
import sys
import daemon

from src import utils, registrations
from setproctitle import setproctitle

last_seen = None
ending = False

class Cancel(Exception):
    ...

async def main(args):
    rabbit = await aio_pika.connect_robust(args.rabbit)
    channel = await rabbit.channel()
    traces_xch = await registrations.Exchanges.FlybyTraces(channel)
    trace_queue = await channel.declare_queue(utils.random_string_with_prefix(f"flyby_trace_{args.icao}_"), exclusive=True)
    await trace_queue.bind(traces_xch, args.icao)

    print(f"Tracing {args.icao}")

    try:
        await asyncio.gather(consume(trace_queue, on_message), session_watcher())
    except Cancel as x:
        await rabbit.close() 
        print(x)

async def consume(queue, on_message):
    async with queue.iterator() as q:
        async for message in q:
            async with message.process():
                await on_message(message)

async def on_message(message):
    global last_seen

    icao = message.headers["icao"]
    body = message.body.decode()
    last_seen = time.time()
    print(f"{icao}: {body}")

async def session_watcher():
    global ending

    dead_count = 0
    while True:
        await asyncio.sleep(5)
        if last_seen is None and dead_count < 3:
            dead_count += 1
            continue

        if dead_count >= 3:
            raise Cancel("Trace never started")

        if ending:
            if time.time() - last_seen > 90:
                raise Cancel("Trace complete")
            elif time.time() - last_seen < 45:
                ending = False
                print("Resuming trace")
                continue

        if time.time() - last_seen < 45:
            continue
        else:
            print("Trace is ending")
            ending = True

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--icao", required=True)
    parser.add_argument("-r", "--rabbit", required=True)
    parser.add_argument("-d", "--daemon", action="store_true")
    args = parser.parse_args()

    if args.daemon:
        with daemon.DaemonContext():
            setproctitle(f"katc: flyby_tracer.py [{args.icao}]")
            asyncio.run(main(args))
    else:
        asyncio.run(main(args))
