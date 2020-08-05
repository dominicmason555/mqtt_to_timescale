import logging
import asyncpg
from pydantic import BaseModel, ValidationError

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


async def weather_setup(conn: asyncpg.connection):
    logging.info("Initialising weather table")
    await conn.execute(QUERY_CREATE_WEATHER)
    await conn.execute(QUERY_HYPER_WEATHER)


async def weather_parse_insert(payload: str, conn: asyncpg.connection):
    try:
        measurement = WeatherMeasurement.parse_raw(payload)
        logging.info(measurement.json())
    except ValidationError as ex:
        logging.warning("Invalid weather measurement, ignoring")
        print(ex)
        return
    try:
        await conn.execute(QUERY_INSERT_WEATHER, measurement.location, measurement.temperature,
                           measurement.pressure, measurement.humidity)
    except asyncpg.InterfaceError as ex:
        logging.critical("DB weather connection failure")
        print(ex)
