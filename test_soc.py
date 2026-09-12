#!/usr/bin/env python3

import os
from pathlib import Path

from nissan_api import NissanSession

ENV_FILE = Path("/etc/nissan-soc-mqtt.env")


def load_env_file(path: Path) -> None:
    if not path.exists():
        raise RuntimeError(f"Credentials file not found: {path}")

    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if not sep:
            continue
        os.environ.setdefault(key.strip(), value.strip())


def main() -> None:
    load_env_file(ENV_FILE)

    username = os.environ.get("NISSAN_USERNAME")
    password = os.environ.get("NISSAN_PASSWORD")
    if not username or not password:
        raise RuntimeError(
            "NISSAN_USERNAME and NISSAN_PASSWORD must be set in "
            f"{ENV_FILE}"
        )

    print("Logging in...")
    session = NissanSession(username, password)
    session.login()
    print("Login OK")

    vehicles = session.vehicles()
    if not vehicles:
        raise RuntimeError("No vehicles found on Nissan account")

    vehicle = vehicles[0]
    model = vehicle.get("modelName") or vehicle.get("model") or "Unknown"
    vin = vehicle["vin"]

    status = session.battery_status(vin)
    soc = status.get("batteryLevel")
    if soc is None:
        raise RuntimeError(f"batteryLevel missing from Nissan response: {status}")

    print(f"Vehicle: {model}")
    print(f"SoC: {soc} %")


if __name__ == "__main__":
    main()
