#!/usr/bin/env python
import aio_pika
import argparse
import asyncio
import time
import sys
import daemon
import logging
import logging.handlers

from src import utils, registrations
from setproctitle import setproctitle

sys.path.append("lib/kazoo")
import kazoo
from kazoo.client import KazooClient

last_seen = None
total_messages = 0
ending = False
zk = None
log = None
args = None

class Cancel(Exception):
    ...

def configure_logs():
    global log

    log = logging.getLogger()
    log.setLevel(logging.INFO)
    syslog = logging.handlers.SysLogHandler(address="/dev/log")
    syslog.ident = "trace.py: "
    syslog.setFormatter(logging.Formatter(f"{args.icao} - %(message)s"))
    log.addHandler(syslog)
    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    console.setLevel(logging.DEBUG)
    log.addHandler(console)

async def main():
    global zk

    configure_logs()

    rabbit = await aio_pika.connect_robust(args.rabbit)
    channel = await rabbit.channel()

    zk = KazooClient(hosts=args.zookeeper)
    zk.start()

    trace_node = f"/katc/{args.icao}_trace"


    try:

        try:
            trace_queue_name = zk.get(trace_node)[0].decode()
            trace_queue = await channel.get_queue(trace_queue_name, ensure=True)
        except (kazoo.exceptions.NoNodeError, aio_pika.exceptions.ChannelClosed):
            raise Cancel(f"Can't find a trace for {args.icao}")

        log.info(f"Tracing {args.icao} using {trace_queue.name}")
        await asyncio.gather(consume(trace_queue, on_message),
                             session_watcher())
    except Cancel as x:
        log.info(x)
    except Exception as x:
        log.error(f"{x.name}: {x}")
    finally:
        log.info(f"Ended after {total_messages} messages")

        zk.delete(trace_node)
        zk.stop()
        await rabbit.close()

async def consume(queue, on_message):
    async with queue.iterator() as q:
        async for message in q:
            async with message.process():
                await on_message(message)

async def on_message(message):
    global last_seen
    global total_messages

    icao = message.headers["icao"]
    body = message.body.decode()
    last_seen = time.time()
    total_messages += 1
    log.debug(f"{icao}: {body}")

async def session_watcher():
    global ending

    dead_count = 0
    while True:
        await asyncio.sleep(5)
        if last_seen is None:
            if dead_count < 6:
                dead_count += 1
                continue
            else:
                raise Cancel("Trace never started")

        if ending:
            if time.time() - last_seen > 360:
                raise Cancel("Trace complete")
            elif time.time() - last_seen < 45:
                ending = False
                log.info("Resuming trace")
                continue

        if time.time() - last_seen < 45:
            continue
        elif not ending:
            ending = True
            log.info("Trace ending")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--icao", required=True)
    parser.add_argument("-r", "--rabbit", required=True)
    parser.add_argument("-d", "--daemon", action="store_true")
    parser.add_argument("-z", "--zookeeper", required=True)
    parser.add_argument("-x", "--exclusive", action="store_true")
    args = parser.parse_args()

    if args.daemon:
        with daemon.DaemonContext():
            setproctitle(f"katc: flyby_tracer.py [{args.icao}]")
            asyncio.run(main())
    else:
        asyncio.run(main())
