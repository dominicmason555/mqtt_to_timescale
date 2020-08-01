import asyncio
import asyncpg
import logging
import toml
import colorama
import coloredlogs
from pydantic import BaseModel, ValidationError
from asyncio_mqtt import Client, MqttError

QUERY_CREATE = """
CREATE TABLE IF NOT EXISTS weather (
    time TIMESTAMPTZ NOT NULL,
    location TEXT NOT NULL,
    temperature REAL NULL,
    pressure REAL NULL,
    humidity REAL NULL
);
"""

QUERY_HYPER = """
SELECT create_hypertable('weather', 'time', if_not_exists => TRUE);
"""

QUERY_INSERT_WEATHER = """
INSERT INTO weather (time, location, temperature, pressure, humidity) VALUES (NOW(), $1, $2, $3, $4)
"""


class ConfigDB(BaseModel):
    user: str
    password: str
    host: str
    port: int
    database: str


class ConfigBroker(BaseModel):
    host: str
    port: int


class Config(BaseModel):
    db: ConfigDB
    broker: ConfigBroker


class WeatherMeasurement(BaseModel):
    location: str
    temperature: float
    pressure: float
    humidity: float


async def db_manager(queue: "asyncio.Queue[WeatherMeasurement]", pool: asyncpg.pool.Pool):
    try:
        async with pool.acquire() as conn:
            # Initialise DB in case it is a fresh instance, these get ignored if already created
            logging.info("Initialising DB table")
            async with conn.transaction():
                await conn.execute(QUERY_CREATE)
                await conn.execute(QUERY_HYPER)
            logging.info("Waiting for measurements")
            while True:
                measurement = await queue.get()
                logging.info(measurement.json())
                async with conn.transaction():
                    await conn.execute(QUERY_INSERT_WEATHER, measurement.location, measurement.temperature,
                                       measurement.pressure, measurement.humidity)
    except asyncpg.InterfaceError as ex:
        logging.critical("DB connection failure")
        print(ex)


async def mqtt_manager(conf: Config, queue: "asyncio.Queue[WeatherMeasurement]"):
    logging.info("Connecting to MQTT broker")
    try:
        async with Client(conf.broker.host) as client:
            logging.info("Connected to MQTT broker")
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
                        logging.critical("Messages backed up, not sending to DB fast enough")
                        return
    except MqttError as ex:
        logging.critical("MQTT Error")
        print(ex)


async def main(conf: Config):
    logging.info("Connecting to database")
    try:
        pool = await asyncpg.create_pool(user=conf.db.user,
                                         password=conf.db.password,
                                         host=conf.db.host,
                                         port=conf.db.port,
                                         database=conf.db.database)
    except Exception as ex:
        logging.critical("Failed to connect to database")
        print(ex)
        return

    logging.info("Connected to database")

    queue: "asyncio.Queue[WeatherMeasurement]" = asyncio.Queue(20)
    _, pending = await asyncio.wait(
        (mqtt_manager(config, queue),
         db_manager(queue, pool)),
        return_when=asyncio.FIRST_COMPLETED
    )
    for task in pending:
        logging.warning("Task Cancelled")
        task.cancel()
    logging.info("Shutting down")


if __name__ == "__main__":
    colorama.init()
    coloredlogs.install(level='DEBUG')
    logging.info("Starting")

    try:
        config_dict = toml.load("config.toml")
        config = Config.parse_obj(config_dict)
        asyncio.run(main(config))

    except ValidationError as e:
        logging.critical("Failed to load config file")
        print(e)
