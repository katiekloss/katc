#!/usr/bin/env python

import asyncio
import argparse
import sys
import daemon
import aio_pika
import subprocess

from src import registrations
from redis import asyncio as aioredis

async def main():
    redis = aioredis.Redis.from_url(args.redis)

    while True:
        icao = await redis.brpop("trace_requests")
        icao = icao[1].decode()
        trace_args = [sys.executable, "./trace.py", "-i", icao, "-r", args.rabbit]
        print(f"Starting trace for {icao}")
        if not await redis.set(f"trace_{icao}", "1", 3600, nx=True):
           print(f"Another trace started, cancelling")
           continue

        try:
            await redis.delete(f"trace_requested_{icao}")
            subprocess.run(trace_args)
        finally:
            await redis.delete(f"trace_{icao}")

if __name__ == "__main__":
    global args

    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--rabbit", required=True)
    parser.add_argument("-s", "--redis", required=True)
    args = parser.parse_args()

    asyncio.run(main())
