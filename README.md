# MQTT Weather to TimescaleDB

Uses [asyncio-mqtt](https://pypi.org/project/asyncio-mqtt/) to pipe MQTT weather sensor measurements to [TimescaleDB](https://www.timescale.com/) using [asyncpg](https://github.com/MagicStack/asyncpg)

Database schema is the following:

| time        | location | temperature | pressure | humidity |
|:-----------:|:--------:|:-----------:|:--------:|:--------:|
| timestamptz | string   | real        | real     | real     |

Configuration file for MQTT and TimescaleDB hosts, usernames and passwords etc is `config.toml`, and it is loaded and validated using [pydantic](https://pydantic-docs.helpmanual.io/).

Sample `config.toml`:

```yaml
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
