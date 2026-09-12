#!/usr/bin/env python3

from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
from pathlib import Path

import paho.mqtt.client as mqtt

from nissan_api import NissanSession

ENV_FILE = Path("/etc/nissan-soc-mqtt.env")

MQTT_HOST = "localhost"
MQTT_PORT = 1883
SOC_TOPIC = "home/ev/mammabim/soc_percent"
CHARGING_TOPIC = "home/ev/mammabim/charging"

EV_METER_TOPIC = "home/ev/meter/status/em:0"

# Keep this aligned with EV_LOW_POWER_THRESHOLD_W in Solis_controller.
CHARGING_POWER_THRESHOLD_W = 50.0

HOURLY_POLL_SECONDS = 60 * 60
CHARGING_POLL_SECONDS = 5 * 60
INITIAL_MQTT_SYNC_SECONDS = 2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
LOG = logging.getLogger("nissan-soc-mqtt")


def load_env_file(path: Path) -> None:
    if not path.exists():
        raise RuntimeError(f"Credentials file not found: {path}")

    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if sep:
            os.environ.setdefault(key.strip(), value.strip())


class App:
    def __init__(self) -> None:
        load_env_file(ENV_FILE)

        username = os.environ.get("NISSAN_USERNAME")
        password = os.environ.get("NISSAN_PASSWORD")
        if not username or not password:
            raise RuntimeError(
                f"NISSAN_USERNAME and NISSAN_PASSWORD must be set in {ENV_FILE}"
            )

        self.nissan = NissanSession(username, password)
        self.vehicle = None

        self.state_lock = threading.Lock()
        self.ev_power_w: float | None = None
        self.api_charging = False
        self.ready = False
        self.events: queue.Queue[str] = queue.Queue()

        try:
            self.mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        except AttributeError:
            self.mqtt = mqtt.Client()

        self.mqtt.on_connect = self.on_connect
        self.mqtt.on_message = self.on_message

    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code != 0:
            LOG.error("MQTT connection failed: %s", reason_code)
            return

        LOG.info("Connected to MQTT broker")
        client.subscribe(EV_METER_TOPIC)

    def meter_charging_locked(self) -> bool:
        return bool(
            self.ev_power_w is not None
            and self.ev_power_w > CHARGING_POWER_THRESHOLD_W
        )

    def should_fast_poll(self) -> bool:
        with self.state_lock:
            return self.meter_charging_locked() and self.api_charging

    def on_message(self, client, userdata, msg):
        if msg.topic != EV_METER_TOPIC:
            return

        payload = msg.payload.decode("utf-8", errors="replace").strip()
        try:
            data = json.loads(payload)
            value = data.get("total_act_power")
            power_w = float(value) if value is not None else None
        except (ValueError, TypeError, json.JSONDecodeError):
            LOG.warning("Invalid EV meter payload")
            return

        with self.state_lock:
            previous_meter_charging = self.meter_charging_locked()
            self.ev_power_w = power_w
            meter_charging = self.meter_charging_locked()
            ready = self.ready

        if not ready or meter_charging == previous_meter_charging:
            return

        if meter_charging:
            LOG.info(
                "EV meter charging started, power=%.1f W; polling Nissan",
                power_w if power_w is not None else -1.0,
            )
            self.events.put("meter_charging_started")
        else:
            LOG.info("EV meter charging stopped; polling Nissan")
            self.events.put("meter_charging_stopped")

    def connect_nissan(self) -> None:
        LOG.info("Logging in to Nissan")
        self.nissan.login()
        vehicles = self.nissan.vehicles()
        if not vehicles:
            raise RuntimeError("No vehicles found on Nissan account")

        self.vehicle = vehicles[0]
        model = self.vehicle.get("modelName") or self.vehicle.get("model") or "Unknown"
        LOG.info("Nissan login OK, vehicle=%s", model)

    @staticmethod
    def nissan_timestamp(status: dict):
        for key in (
            "timestamp",
            "lastUpdateTime",
            "lastUpdatedTime",
            "batteryStatusLastUpdated",
        ):
            value = status.get(key)
            if value:
                return value
        return None

    @staticmethod
    def plug_label(value) -> str:
        return {0: "NOT_PLUGGED", 1: "PLUGGED"}.get(value, f"UNKNOWN({value})")

    @staticmethod
    def charge_label(value) -> str:
        return {0: "NOT_CHARGING", 1: "CHARGING"}.get(value, f"UNKNOWN({value})")

    def publish_retained(self, topic: str, payload: str) -> None:
        result = self.mqtt.publish(topic, payload, qos=1, retain=True)
        result.wait_for_publish(timeout=10)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"MQTT publish failed for {topic}: {result.rc}")

    def fetch_and_publish(self, reason: str) -> bool:
        try:
            if self.vehicle is None:
                self.connect_nissan()

            status = self.nissan.battery_status(self.vehicle["vin"])
            soc = status.get("batteryLevel")
            if soc is None:
                raise RuntimeError("batteryLevel missing from Nissan response")

            plug_status = status.get("plugStatus")
            charge_status = status.get("chargeStatus")
            plug_label = self.plug_label(plug_status)
            charge_label = self.charge_label(charge_status)

            with self.state_lock:
                self.api_charging = charge_status == 1

            self.publish_retained(SOC_TOPIC, str(soc))
            self.publish_retained(CHARGING_TOPIC, charge_label)

            updated = self.nissan_timestamp(status)
            LOG.info(
                "SoC=%s%% plugged=%s charging=%s Nissan_updated=%s reason=%s",
                soc,
                plug_label,
                charge_label,
                updated or "unknown",
                reason,
            )
            return True
        except Exception:
            LOG.exception("Nissan fetch failed, reason=%s", reason)
            return False

    def run(self) -> None:
        self.mqtt.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        self.mqtt.loop_start()

        try:
            time.sleep(INITIAL_MQTT_SYNC_SECONDS)
            with self.state_lock:
                self.ready = True
                power_w = self.ev_power_w
                meter_charging = self.meter_charging_locked()

            LOG.info(
                "Initial MQTT state synced, power=%s",
                f"{power_w:.1f} W" if power_w is not None else "unknown",
            )

            # Startup fetch gives Home Assistant current SoC/status. If charging is
            # already in progress, this same fetch also identifies Mammabim.
            self.fetch_and_publish("startup_charging" if meter_charging else "startup")

            while True:
                timeout = (
                    CHARGING_POLL_SECONDS
                    if self.should_fast_poll()
                    else HOURLY_POLL_SECONDS
                )

                try:
                    reason = self.events.get(timeout=timeout)
                except queue.Empty:
                    reason = (
                        "scheduled_charging"
                        if self.should_fast_poll()
                        else "scheduled_hourly"
                    )

                # Multiple rapid meter transitions (for example from clouds) do not
                # need one API request per queued edge. One fresh poll is enough.
                while True:
                    try:
                        pending = self.events.get_nowait()
                    except queue.Empty:
                        break
                    reason = f"{reason}+{pending}"

                self.fetch_and_publish(reason)
        finally:
            self.mqtt.loop_stop()
            self.mqtt.disconnect()


def main() -> None:
    App().run()


if __name__ == "__main__":
    main()
