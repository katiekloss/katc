#!/usr/bin/env python
import aio_pika
import argparse
import asyncio
import time
import sys
import daemon
import logging
import logging.handlers
import pyModeS as pms
from redis import Redis

from src import utils, registrations
from setproctitle import setproctitle

last_position = (41.4072, -81.8573)
last_position_message_ts = None
last_position_message = ""
last_seen = None
total_messages = 0
ending = False
log = None
args = None
callsign = None
forward_xch = None

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
    global forward_xch

    configure_logs()

    rabbit = await aio_pika.connect_robust(args.rabbit)
    channel = await rabbit.channel()

    lock_key = f"/katc/{args.icao}_trace"

    redis = Redis.from_url(args.redis)
    val = redis.set(lock_key, "trace", get=True)
    if val != None and val.decode() == "trace":
       raise Cancel(f"Conflicted over {args.icao}, quitting")

    try:
        log.info(f"Locked {args.icao}")
        trace_queue = await channel.declare_queue(utils.random_string_with_prefix(f"trace_{args.icao}_"),
                                                        durable=False,
                                                        auto_delete=True,
                                                        arguments={"x-expires": 5 * 60 * 1000})

        forward_xch = await registrations.Exchanges.TracesForHumans(channel)
        trace_xch = await registrations.Exchanges.ADSB(channel)
        await trace_queue.bind(trace_xch, f"icao.{args.icao}.#")

        log.info(f"Tracing {args.icao} using {trace_queue.name}")

        await asyncio.gather(consume(trace_queue, on_message),
                             session_watcher())
    except Cancel as x:
        log.info(x)
    except Exception as x:
        log.error(x)
    except asyncio.exceptions.CancelledError:
        ...
    finally:
        try:
            redis.delete(lock_key)
        except:
            log.warning(f"Couldn't unlock {args.icao}")

        log.info(f"Ended after {total_messages} messages")

        try:
            await trace_queue.delete()
        except:
            ...
        await rabbit.close()

async def consume(queue, on_message):
    async with queue.iterator() as q:
        async for message in q:
            async with message.process():
                await on_message(message)

async def on_message(message):
    global last_position
    global last_position_message
    global last_position_message_ts
    global last_seen
    global total_messages
    global callsign

    icao = message.headers["icao"]
    tc = message.headers["typecode"]
    body = message.body.decode()

    now = time.time()
    log.debug(f"{icao}: {body}")

    if tc == None:
        log.warning(f"No TC from {icao}: {body}")
        return

    meaning = ""
    if 1 <= tc <= 4 and callsign == None:
        try:
            callsign = pms.adsb.callsign(body).rstrip("_ ")
        except:
            log.warning(f"Ident message without callsign: {body}")
            return

        meaning = f"{icao} is now {callsign}"

    if 9 <= tc <= 18 or 20 <= tc <= 22:
        if last_position_message == "":
            last_position = pms.adsb.position_with_ref(body, last_position[0], last_position[1])
            last_position_message = body
            last_position_message_ts = now
        elif pms.adsb.oe_flag(last_position_message) != pms.adsb.oe_flag(body):
            last_position = pms.adsb.position(last_position_message, body, last_position_message_ts, now)
            last_position_message = body
            last_position_message_ts = now

        altitude = pms.adsb.altitude(body)
        if tc <= 18:
            meaning = f"position {last_position}, at {altitude} ft"
        else:
            meaning = f"position {last_position}, at {altitude} m"
    elif tc == 19:
        velocity = pms.adsb.velocity(body)
        speed = int(velocity[0])
        heading = int(velocity[1])
        meaning = f"speed {speed} kt/s heading {heading}"
    elif tc == 28:
        squawk = pms.adsb.emergency_squawk(body)
        meaning = f"squawk {squawk}"
    elif tc == 29:
        target_altitude = pms.adsb.selected_altitude(body)[0]
        meaning = f"target altitude {target_altitude} ft"
    elif tc == 31:
        meaning = "operational status here"
    if meaning != "":
        meaning = f"{icao}/{callsign} sent {tc}: {body} - {meaning}"
        await forward_xch.publish(aio_pika.Message(meaning.encode(),
                                                   headers={"icao": icao,
                                                            "typecode": tc}),
                                  icao)

    last_seen = time.time()
    total_messages += 1

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
    parser.add_argument("-s", "--redis", required=True)
    parser.add_argument("-x", "--exclusive", action="store_true")
    args = parser.parse_args()

    if args.daemon:
        with daemon.DaemonContext():
            setproctitle(f"katc: trace.py [{args.icao}]")
            asyncio.run(main())
    else:
        asyncio.run(main())
