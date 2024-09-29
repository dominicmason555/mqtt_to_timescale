import logging
import asyncpg
from pydantic import BaseModel, ValidationError

QUERY_CREATE_GAS = """
CREATE TABLE IF NOT EXISTS gas (
    time TIMESTAMPTZ NOT NULL,
    location TEXT NOT NULL,
    tvoc SMALLINT NULL,
    eco2 SMALLINT NULL
);
"""

QUERY_HYPER_GAS = """
SELECT create_hypertable('gas', 'time', if_not_exists => TRUE);
"""

QUERY_INSERT_GAS = """
INSERT INTO gas (time, location, tvoc, eco2) VALUES (NOW(), $1, $2, $3)
"""


class GasMeasurement(BaseModel):
    location: str
    tvoc: int
    eco2: int


async def gas_setup(conn: asyncpg.connection):
    logging.info("Initialising gas table")
    await conn.execute(QUERY_CREATE_GAS)
    await conn.execute(QUERY_HYPER_GAS)


async def gas_parse_insert(payload: str, conn: asyncpg.connection):
    try:
        measurement = GasMeasurement.parse_raw(payload)
        logging.info(measurement.json())
    except ValidationError as ex:
        logging.warning("Invalid gas measurement, ignoring")
        print(ex)
        return
    try:
        await conn.execute(
            QUERY_INSERT_GAS, measurement.location, measurement.tvoc, measurement.eco2
        )
    except asyncpg.InterfaceError as ex:
        logging.critical("DB gas connection failure")
        print(ex)
