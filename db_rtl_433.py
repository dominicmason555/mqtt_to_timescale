import logging
import asyncpg
import datetime

from pydantic import BaseModel, ValidationError

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

ALLOWED_DATA = ["2e4f8d8"]


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


async def rtl_433_setup(conn: asyncpg.connection):
    logging.info("Initialising RTL433 DB table")
    await conn.execute(QUERY_CREATE_RTL433)
    await conn.execute(QUERY_HYPER_RTL433)


async def rtl_433_parse_insert(payload: str, conn: asyncpg.connection):
    try:
        measurement = RTL433Event.parse_raw(payload)
        # Time is already in UTC as specified in RTL433 config file, so ensure it doesn't get assumed to be local time
        measurement.time = measurement.time.replace(tzinfo=datetime.timezone.utc)
        logging.info(measurement.json())
        if measurement.data not in ALLOWED_DATA:
            logging.info("Got unrecognised RTL433 data string, ignoring")
            return
    except ValidationError as ex:
        logging.warning("Invalid RTL433 event, ignoring")
        print(ex)
        return
    try:
        await conn.execute(QUERY_INSERT_RTL433, measurement.time, measurement.model, measurement.count,
                           measurement.num_rows, measurement.len, measurement.data, measurement.rssi,
                           measurement.snr, measurement.noise)
    except asyncpg.InterfaceError as ex:
        logging.critical("DB RTL433 connection failure")
        print(ex)
