# MQTT to TimescaleDB

Uses [asyncio-mqtt](https://pypi.org/project/asyncio-mqtt/) to pipe MQTT events and sensor data to [TimescaleDB](https://www.timescale.com/) using [asyncpg](https://github.com/MagicStack/asyncpg) after validating using [pydantic](https://pydantic-docs.helpmanual.io/).

Currently stores measurements from weather sensors, and events from RTL_433 e.g. 433 Mhz doorbell or motion sensor, but it should be simple to add additional MQTT subscriptions and database tables.

Configuration file for MQTT and TimescaleDB hosts, usernames and passwords etc is `config.toml`, and it is loaded and validated using [pydantic](https://pydantic-docs.helpmanual.io/).

Sample `config.toml`:

```ini
[db]
user = "username"
password = "password"
host = "127.0.0.1"
port = 5432
database = "database"

[broker]
host = "127.0.0.1"
port = 1883
```

Can be built to a native executable using `python setup.py build` after installing packages using `pip install -r requirements.txt`
