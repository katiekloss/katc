import aio_pika
import os
import random
import string

async def connect_to_rabbit():
    try:
        conn_string = os.environ["RABBIT_CONNECTION_STRING"]
    except KeyError:
        raise KeyError("I need a Rabbit connection string in RABBIT_CONNECTION_STRING")

    return await aio_pika.connect_robust(conn_string)

def random_string_with_prefix(prefix, random_string_length = 10):
    return prefix + ''.join(random.choices(string.ascii_lowercase + string.digits, k=random_string_length))
