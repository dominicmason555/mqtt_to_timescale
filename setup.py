from cx_Freeze import setup, Executable
import sys

build_exe_options = {
    "packages": [
        "asyncio",
        "asyncpg",
        "toml",
        "colorama",
        "coloredlogs",
        "asyncio_mqtt",
        "pydantic",
        "ipaddress",
        "colorsys"
    ],
    "excludes": ["tkinter", "tkconstants", "tcl", "tk"],
    "include_files": "config.toml",
    "build_exe": "build",
    "optimize": 2
}

executable = [
    Executable("mqtt_to_timescale.py",
               targetName="mqtt_to_timescale"
               )
]

setup(name="Main",
      version="0.2",
      description="test",
      options={"build_exe": build_exe_options},
      executables=executable
      )
