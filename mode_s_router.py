#!/usr/bin/env python
import aio_pika
import asyncio
import os
import pyModeS as pms

async def main():
    try:
        conn_string = os.environ["RABBIT_CONNECTION_STRING"]
    except KeyError:
        print("I need a Rabbit connection string in RABBIT_CONNECTION_STRING")
        return

    rabbit = await aio_pika.connect_robust(conn_string)

    channel = await rabbit.channel()
    mode_s_exchange = await channel.declare_exchange("mode_s_by_downlink",
                                                     aio_pika.ExchangeType.TOPIC,
                                                     durable = True)
    adsb_exchange = await channel.declare_exchange("adsb",
                                                   aio_pika.ExchangeType.TOPIC,
                                                   durable = True)

    drop_queue = await channel.declare_queue("mode_s_by_downlink_default",
                                        durable = False,
                                        arguments = {"x-max-length": 1, "x-overflow": "drop-head"})

    await drop_queue.bind(mode_s_exchange, "*")

    await channel.set_qos(prefetch_count = 10)

    queue = await channel.declare_queue("mode_s_router",
                                        durable = True)

    await queue.bind("mode_s", "raw")

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                await route(message, mode_s_exchange, adsb_exchange)

async def route(message, mode_s_exchange, adsb_exchange):
    data = message.body.decode()
    try:
        df = pms.df(data)
    except:
        print(f"Failed to decode: {data}")
        return

    try:
        icao = pms.icao(data)
    except:
        print(f"No ICAO on {df}: {data}")
        return

    tc = pms.adsb.typecode(data)
    if tc == None and 17 <= df <= 18:
        print(f"No typecode ({df}): {data}")
        return

    routed_message = aio_pika.Message(message.body,
                                      headers = {"icao": icao,
                                                 "typecode": tc,
                                                 "downlink": df})
    
    await mode_s_exchange.publish(routed_message, routing_key = str(df))
    if df == 17 or df == 18:
        await adsb_exchange.publish(routed_message, f"icao.{icao}.typecode.{tc}")

if __name__ == "__main__":
    asyncio.run(main())
