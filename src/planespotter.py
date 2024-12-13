#!/usr/bin/env python

import aio_pika
import asyncio
import logging
import pyModeS as pms

from src import utils

callsigns = dict()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(console)

rpc_exc = None

async def main():
    global rpc_exc

    broker = await utils.connect_to_rabbit()
    channel = await broker.channel()

    xch = await channel.get_exchange("adsb")
    adsb_queue = await channel.declare_queue("", durable = False, exclusive = True)
    for tc in range(1, 5):
        await adsb_queue.bind(xch, f"icao.#.typecode.{tc}")

    rpc_exc = await channel.declare_exchange("rpc", aio_pika.ExchangeType.DIRECT, durable = True)
    rpc_queue = await channel.declare_queue("get-callsign")
    await rpc_queue.bind(rpc_exc)

    await asyncio.gather(consume(rpc_queue, on_rpc, True),
                         consume(adsb_queue, on_message))

async def consume(queue, on_message, no_ack = False):
    async with queue.iterator(no_ack = no_ack) as q:
        async for message in q:
            if no_ack:
                await on_message(message)
            else:
                async with message.process():
                    await on_message(message)

async def on_rpc(message):
    logger.debug(f"RPC {message.correlation_id}: {message.routing_key}({message.body.decode()}) -> {message.reply_to}")
    if message.routing_key == "get-callsign":
        icao = message.body.decode()
        if icao in callsigns:
            await rpc_exc.publish(aio_pika.Message(callsigns[icao].encode(),
                                                   correlation_id = message.correlation_id),
                                  message.reply_to)

async def on_message(message):
    tc = message.headers["typecode"]
    icao = message.headers["icao"]
    data = message.body.decode()
    try:
        callsign = pms.adsb.callsign(data).rstrip("_")
    except:
        logger.warn(f"No callsign: {data}")
        return
    
    if icao in callsigns:
        return

    callsigns[icao] = callsign
    logger.info(f"{icao} -> {callsign}")

if __name__ == "__main__":
    asyncio.run(main())
