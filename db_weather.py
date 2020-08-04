import asyncio
import logging
import asyncpg
from pydantic import BaseModel, ValidationError
from asyncio_mqtt import Client, MqttError

QUERY_CREATE_WEATHER = """
CREATE TABLE IF NOT EXISTS weather (
    time TIMESTAMPTZ NOT NULL,
    location TEXT NOT NULL,
    temperature REAL NULL,
    pressure REAL NULL,
    humidity REAL NULL
);
"""

QUERY_HYPER_WEATHER = """
SELECT create_hypertable('weather', 'time', if_not_exists => TRUE);
"""

QUERY_INSERT_WEATHER = """
INSERT INTO weather (time, location, temperature, pressure, humidity) VALUES (NOW(), $1, $2, $3, $4)
"""


class WeatherMeasurement(BaseModel):
    location: str
    temperature: float
    pressure: float
    humidity: float


async def mqtt_weather_manager(client: Client, queue: "asyncio.Queue[WeatherMeasurement]"):
    try:
        async with client.filtered_messages("timescaledb/weather") as messages:
            await client.subscribe("timescaledb/weather")
            async for message in messages:
                try:
                    parsed = WeatherMeasurement.parse_raw(message.payload)
                    queue.put_nowait(parsed)
                except ValidationError as ex:
                    logging.warning("Invalid weather measurement, ignoring")
                    print(ex)
                except asyncio.QueueFull:
                    logging.critical("Weather measurements backed up, not sending to DB fast enough")
                    return
    except MqttError as ex:
        logging.critical("MQTT Error in weather handler")
        print(ex)


async def db_weather_manager(queue: "asyncio.Queue[WeatherMeasurement]", pool: asyncpg.pool.Pool):
    try:
        async with pool.acquire() as conn:
            # Initialise DB in case it is a fresh instance, these get ignored if already created
            logging.info("Initialising weather DB table")
            async with conn.transaction():
                await conn.execute(QUERY_CREATE_WEATHER)
                await conn.execute(QUERY_HYPER_WEATHER)

        logging.info("Waiting for weather measurements")
        while True:
            measurement = await queue.get()
            logging.info(measurement.json())
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(QUERY_INSERT_WEATHER, measurement.location, measurement.temperature,
                                       measurement.pressure, measurement.humidity)
    except asyncpg.InterfaceError as ex:
        logging.critical("DB weather connection failure")
        print(ex)
