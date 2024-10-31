import aio_pika
import os

async def connect_to_rabbit():
    try:
        conn_string = os.environ["RABBIT_CONNECTION_STRING"]
    except KeyError:
        raise KeyError("I need a Rabbit connection string in RABBIT_CONNECTION_STRING")

    return await aio_pika.connect_robust(conn_string)
