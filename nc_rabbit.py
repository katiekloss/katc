#!/usr/bin/env python
import sys
import aio_pika
import aio_pika.abc
import asyncio
import time
import os

async def dump1090_loop() -> None:
    try:
        conn_string = os.environ["RABBIT_CONNECTION_STRING"]
    except KeyError:
        print("I need a Rabbit connection string in RABBIT_CONNECTION_STRING")
        return

    rabbit: aio_pika.RobustConnection = await aio_pika.connect_robust(conn_string)
    channel: aio_pika.abc.AbstractChannel = await rabbit.channel()
    exchange = await channel.get_exchange("mode_s", ensure = True)

    try:
        source_ip = os.environ["DUMP1090_IP_ADDR"]
    except KeyError:
        print("I need dump1090's IP address in DUMP1090_IP_ADDR")
        return

    reader, writer = await asyncio.open_connection(source_ip, 30002)
    
    async for line in reader:
        now = time.time_ns()
        line = line.rstrip()[1:-1]
                
        await exchange.publish(aio_pika.Message(body = line,
                                                timestamp = time.time()),
                                                routing_key = 'raw')

asyncio.run(dump1090_loop())
