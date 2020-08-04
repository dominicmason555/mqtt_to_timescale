import asyncio
import logging
import asyncpg
import datetime
from pydantic import BaseModel, ValidationError
from asyncio_mqtt import Client, MqttError

QUERY_CREATE_RTL433 = """
CREATE TABLE IF NOT EXISTS RTL433 (
    time TIMESTAMPTZ NOT NULL,
    model TEXT NOT NULL,
    count INTEGER NOT NULL,
    num_rows INTEGER NOT NULL,
    len INTEGER NOT NULL,
    data TEXT NOT NULL,
    rssi REAL NOT NULL,
    snr REAL NOT NULL,
    noise REAL NOT NULL
);
"""

QUERY_HYPER_RTL433 = """
SELECT create_hypertable('RTL433', 'time', if_not_exists => TRUE);
"""

QUERY_INSERT_RTL433 = """
INSERT INTO RTL433 (time, model, count, num_rows, len, data, rssi, snr, noise) 
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
"""


class RTL433Event(BaseModel):
    time: datetime.datetime
    model: str
    count: int
    num_rows: int
    len: int
    data: str
    rssi: float
    snr: float
    noise: float


async def mqtt_rtl_433_manager(client: Client, queue: "asyncio.Queue[RTL433Event]"):
    try:
        async with client.filtered_messages("timescaledb/rtl433") as messages:
            await client.subscribe("timescaledb/rtl433")
            async for message in messages:
                try:
                    parsed = RTL433Event.parse_raw(message.payload)
                    queue.put_nowait(parsed)
                except ValidationError as ex:
                    logging.warning("Invalid RTL433 event, ignoring")
                    print(ex)
                except asyncio.QueueFull:
                    logging.critical("RTL433 events backed up, not sending to DB fast enough")
                    return

    except MqttError as ex:
        logging.critical("MQTT Error in RTL433 handler")
        print(ex)


async def db_rtl_433_manager(queue: "asyncio.Queue[RTL433Event]", pool: asyncpg.pool.Pool):
    try:
        async with pool.acquire() as conn:
            # Initialise DB in case it is a fresh instance, these get ignored if already created
            logging.info("Initialising RTL433 DB table")
            async with conn.transaction():
                await conn.execute(QUERY_CREATE_RTL433)
                await conn.execute(QUERY_HYPER_RTL433)

        logging.info("Waiting for RTL433 events")
        while True:
            measurement = await queue.get()
            logging.info(measurement.json())
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(QUERY_INSERT_RTL433, measurement.time, measurement.model, measurement.count,
                                       measurement.num_rows, measurement.len, measurement.data, measurement.rssi,
                                       measurement.snr, measurement.noise)
    except asyncpg.InterfaceError as ex:
        logging.critical("DB RTL433 connection failure")
        print(ex)
