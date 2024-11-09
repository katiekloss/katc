import aio_pika

class Exchanges:
    async def ADSB(channel):
        return await channel.declare_exchange("adsb", aio_pika.ExchangeType.TOPIC, durable=True)

    async def RPC(channel):
        return await channel.declare_exchange("rpc", aio_pika.ExchangeType.DIRECT, durable=True)

    async def FlightStateChanges(channel):
        return await channel.declare_exchange("flight_state_changed", aio_pika.ExchangeType.TOPIC, durable=True)

    async def FlybyTraces(channel):
        return await channel.declare_exchange("flyby_traces",
                                              aio_pika.ExchangeType.DIRECT,
                                              durable=True,
                                              arguments={"alternate-exchange": "flyby_traces_unrouted"})

    async def FlybyTracesUnrouted(channel):
        return await channel.declare_exchange("flyby_traces_unrouted", aio_pika.ExchangeType.FANOUT, durable=True)

class Queues:
    ...
