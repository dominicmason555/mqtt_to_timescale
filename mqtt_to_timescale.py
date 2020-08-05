import asyncio
import logging
from typing import Callable, Awaitable

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


async def mqtt_db_manager(client: Client, pool: asyncpg.pool.Pool, topic: str,
                          setup_table: Callable[[asyncpg.connection.Connection], Awaitable[None]],
                          parse_insert: Callable[[str, asyncpg.connection.Connection], Awaitable[None]]):
    async with pool.acquire() as conn:
        # Initialise DB in case it is a fresh instance, these get ignored if already created
        async with conn.transaction():
            await setup_table(conn)
    try:
        async with client.filtered_messages(topic, queue_maxsize=10) as messages:
            await client.subscribe(topic)
            async for message in messages:
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        await parse_insert(message.payload, conn)
    except MqttError as ex:
        logging.critical("MQTT Error")
        print(ex)
    except asyncpg.InterfaceError as ex:
        logging.critical("DB connection failure")
        print(ex)


async def run(client: Client, pool: asyncpg.pool):
    _, pending = await asyncio.wait(
        (mqtt_db_manager(client, pool, "timescaledb/weather", db_weather.weather_setup,
                         db_weather.weather_parse_insert),
         mqtt_db_manager(client, pool, "timescaledb/rtl433", db_rtl_433.rtl_433_setup,
                         db_rtl_433.rtl_433_parse_insert)),
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
        await pool.close()


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
