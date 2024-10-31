#!/usr/bin/env python

import asyncio
import pyModeS as pms

from src import utils

callsign_map = dict()

async def main():
    broker = await utils.connect_to_rabbit()
    channel = await broker.channel()
    exc = await channel.get_exchange("mode_s_by_downlink")

    queue = await channel.declare_queue("mode_s_tracker", durable = False, exclusive = True)
    await queue.bind(exc, "17")
    await queue.bind(exc, "18")

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                await do_message(message)

async def do_message(message):
    body = message.body.decode()
    try:
        icao = pms.icao(body)
    except:
        print(f"No ICAO: {body}")
        return

    tc = pms.adsb.typecode(body)
    if tc == None:
        print(f"No TC from {icao}: {body}")
        return

    meaning = ""
    if 1 <= tc <= 4 and icao not in callsign_map:
        try:
            callsign = pms.adsb.callsign(body).rstrip("_ ")
        except:
            print(f"Ident message without callsign: {body}")
            return

        callsign_map[icao] = callsign
        meaning = f"{icao} is now {callsign}"
    elif icao in callsign_map:
        callsign = callsign_map[icao]
    else:
        callsign = "unknown"

    if 5 <= tc <= 18 or 20 <= tc <= 22:
        altitude = pms.adsb.altitude(body)
        if tc <= 18:
            meaning = f"at {altitude} ft"
        else:
            meaning = f"at {altitude} m"
    elif tc == 19:
        heading = pms.adsb.velocity(body)
        meaning = f"speed {heading[0]} kt/s heading {heading[1]}"

    print(f"{icao} ({callsign}) sent {tc} on {message.routing_key}: {body} - {meaning}")


if __name__ == "__main__":
    asyncio.run(main())
