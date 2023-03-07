import logging
import asyncpg
from pydantic import BaseModel, ValidationError


QUERY_CREATE_GOSUND_SENSOR = """
CREATE TABLE IF NOT EXISTS gosund_sensor (
    time TIMESTAMPTZ NOT NULL,
    location TEXT NOT NULL,
    total REAL NULL,
    yesterday REAL NULL,
    today REAL NULL,
    period REAL NULL,
    power REAL NULL,
    apparent REAL NULL,
    reactive REAL NULL,
    factor REAL NULL,
    voltage REAL NULL,
    current REAL NULL
);
"""

QUERY_HYPER_GOSUND_SENSOR = """
SELECT create_hypertable('gosund_sensor', 'time', if_not_exists => TRUE);
"""

QUERY_INSERT_GOSUND_SENSOR = """
INSERT INTO gosund_sensor (
    time, location, total, yesterday, today, period,
    power, apparent, reactive, factor, voltage, current
) VALUES (NOW(), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
"""


class GosundSensorMeasurement(BaseModel):
    location: str
    total: float
    yesterday: float
    today: float
    period: float
    power: float
    apparent: float
    reactive: float
    factor: float
    voltage: float
    current: float


async def gosund_sensor_setup(conn: asyncpg.connection):
    logging.info("Initialising gosund sensor table")
    await conn.execute(QUERY_CREATE_GOSUND_SENSOR)
    await conn.execute(QUERY_HYPER_GOSUND_SENSOR)


async def gosund_sensor_parse_insert(payload: str, conn: asyncpg.connection):
    try:
        measurement = GosundSensorMeasurement.parse_raw(payload)
        logging.info(measurement.json())
    except ValidationError as ex:
        logging.warning("Invalid gosund sensor measurement, ignoring")
        print(ex)
        return
    try:
        await conn.execute(
            QUERY_INSERT_GOSUND_SENSOR,
            measurement.location,
            measurement.total,
            measurement.yesterday,
            measurement.today,
            measurement.period,
            measurement.power,
            measurement.apparent,
            measurement.reactive,
            measurement.factor,
            measurement.voltage,
            measurement.current
        )
    except asyncpg.InterfaceError as ex:
        logging.critical("DB gosund sensor connection failure")
        print(ex)
