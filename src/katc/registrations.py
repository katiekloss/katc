import aio_pika

class Exchanges:
    async def ADSB(channel):
        return await channel.declare_exchange("adsb", aio_pika.ExchangeType.TOPIC, durable=True)

    async def RPC(channel):
        return await channel.declare_exchange("rpc", aio_pika.ExchangeType.DIRECT, durable=True)

    async def FlightStateChanges(channel):
        return await channel.declare_exchange("flight_state_changed", aio_pika.ExchangeType.TOPIC, durable=True)

    async def TraceDispatch(channel):
        return await channel.declare_exchange("trace_dispatch",
                                              aio_pika.ExchangeType.FANOUT,
                                              durable=True)
    async def TracesForHumans(channel):
        return await channel.declare_exchange("traces_for_humans",
                                              aio_pika.ExchangeType.FANOUT,
                                              durable=True)

class Queues:
    ...
