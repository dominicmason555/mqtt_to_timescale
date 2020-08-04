import asyncio
import logging

import asyncpg
import colorama
import coloredlogs
import toml
from asyncio_mqtt import Client, MqttError
from pydantic import BaseModel, ValidationError

import db_rtl_433
import db_weather


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


async def run(client: Client, pool: asyncpg.pool):
    weather_queue: "asyncio.Queue[db_weather.WeatherMeasurement]" = asyncio.Queue(20)
    rtl_433_queue: "asyncio.Queue[db_rtl_433.RTL433Event]" = asyncio.Queue(20)

    _, pending = await asyncio.wait(
        (db_weather.mqtt_weather_manager(client, weather_queue),
         db_weather.db_weather_manager(weather_queue, pool),
         db_rtl_433.mqtt_rtl_433_manager(client, rtl_433_queue),
         db_rtl_433.db_rtl_433_manager(rtl_433_queue, pool)),
        return_when=asyncio.FIRST_COMPLETED
    )
    for task in pending:
        logging.warning("Task Cancelled")
        task.cancel()
    logging.info("Shutting down")


async def main(conf: Config):
    logging.info("Connecting to database")
    try:
        pool = await asyncpg.create_pool(user=conf.db.user,
                                         password=conf.db.password,
                                         host=conf.db.host,
                                         port=conf.db.port,
                                         database=conf.db.database,
                                         min_size=2)
    except asyncpg.InterfaceError as ex:
        logging.critical("Failed to connect to database")
        print(ex)
        return

    logging.info("Connected to database")

    logging.info("Connecting to MQTT broker")
    try:
        async with Client(conf.broker.host) as client:
            logging.info("Connected to MQTT broker")

            await run(client, pool)

    except MqttError as ex:
        logging.critical("MQTT Error")
        print(ex)
    finally:
        pool.close()


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
