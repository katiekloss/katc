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
    publish_exchange = await channel.declare_exchange("mode_s_by_downlink",
                                                              aio_pika.ExchangeType.TOPIC,
                                                              durable = True)

    drop_queue = await channel.declare_queue("mode_s_by_downlink_default",
                                        durable = False,
                                        arguments = {"x-max-length": 1, "x-overflow": "drop-head"})
    await drop_queue.bind(publish_exchange, "*")

    await channel.set_qos(prefetch_count = 10)

    queue = await channel.declare_queue("mode_s_router",
                                        durable = True)

    await queue.bind("mode_s", "raw")

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                await route(message, publish_exchange)

async def route(message, downlink_exchange):
    try:
        df = pms.df(message.body.decode())
    except:
        print(f"Failed to decode: {message.body}")
        return
    
    await downlink_exchange.publish(message, routing_key = str(df))

if __name__ == "__main__":
    asyncio.run(main())
