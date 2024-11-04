#!/usr/bin/env python

import asyncio
import pyModeS as pms
import aio_pika
import uuid
import concurrent.futures

from src import utils

callsign_map = dict()
rpcs = dict()
rpc_exc = None
rpc_id = None

async def main():
    global rpc_exc
    global rpc_id

    broker = await utils.connect_to_rabbit()
    channel = await broker.channel()
    exc = await channel.declare_exchange("adsb", aio_pika.ExchangeType.TOPIC, durable = True)

    rpc_exc = await channel.declare_exchange("rpc", aio_pika.ExchangeType.DIRECT, durable = True)

    queue = await channel.declare_queue("tracker", durable = False, exclusive = True)
    await queue.bind(exc, "#")

    rpc_queue = await channel.declare_queue("", exclusive = True)
    rpc_id = rpc_queue.name
    await rpc_queue.bind(rpc_exc)

    await asyncio.gather(consume(queue, on_adsb),
                         consume(rpc_queue, on_rpc, no_ack = True))

async def consume(queue, on_message, no_ack = False):
    async with queue.iterator(no_ack = no_ack) as q:
        async for message in q:
            if no_ack:
                await on_message(message)
            else:
                async with message.process():
                    await on_message(message)

async def call_rpc(method_name, args):
    call_id = str(uuid.uuid4())

    await rpc_exc.publish(aio_pika.Message(args.encode(),
                                           reply_to = rpc_id,
                                           correlation_id = call_id),
                          method_name)

    rpcs[call_id] = future = asyncio.get_event_loop().create_future()
    try:
        result = await asyncio.wait_for(future, 2)
        del rpcs[call_id]
        return future.result()
    except TimeoutError:
        del rpcs[call_id]
        raise TimeoutError(f"RPC {method_name}({args}) timed out ({call_id})")

async def on_rpc(message):
    if message.correlation_id in rpcs:
        future = rpcs[message.correlation_id]
        future.set_result(message.body.decode())

async def on_adsb(message):
    body = message.body.decode()
    icao = message.headers["icao"]
    tc = message.headers["typecode"]
    if tc == None:
        print(f"No TC from {icao}: {body}")
        return

    if icao not in callsign_map:
        try:
            callsign = await call_rpc("get-callsign", icao)
            if callsign != "":
                callsign_map[icao] = callsign
        except TimeoutError:
            pass

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

    print(f"{icao} ({callsign}) sent {tc}: {body} - {meaning}")


if __name__ == "__main__":
    asyncio.run(main())
