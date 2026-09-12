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
CAR_NAME = "Mammabim"
SOC_TOPIC = "home/ev/mammabim/soc_percent"

EVENT_TOPIC = "home/ev/control/event"
ACTIVE_CANDIDATE_TOPIC = "home/ev/control/active_candidate"
GARAGE_CAR_TOPIC = "home/ev/garage/car"
CARPORT_CAR_TOPIC = "home/ev/carport/car"
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
        self.state = {
            ACTIVE_CANDIDATE_TOPIC: None,
            GARAGE_CAR_TOPIC: None,
            CARPORT_CAR_TOPIC: None,
        }
        self.ev_power_w: float | None = None
        self.ready = False
        self.was_charging = False
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
        client.subscribe(
            [
                (EVENT_TOPIC, 0),
                (ACTIVE_CANDIDATE_TOPIC, 0),
                (GARAGE_CAR_TOPIC, 0),
                (CARPORT_CAR_TOPIC, 0),
                (EV_METER_TOPIC, 0),
            ]
        )

    def selected_car_locked(self) -> str | None:
        candidate = self.state[ACTIVE_CANDIDATE_TOPIC]
        if candidate == "charger1":
            return self.state[GARAGE_CAR_TOPIC]
        if candidate == "charger2":
            return self.state[CARPORT_CAR_TOPIC]
        return None

    def is_charging_locked(self) -> bool:
        return bool(
            self.ev_power_w is not None
            and self.ev_power_w > CHARGING_POWER_THRESHOLD_W
            and self.selected_car_locked() == CAR_NAME
        )

    def is_charging(self) -> bool:
        with self.state_lock:
            return self.is_charging_locked()

    def update_charging_transition_locked(self) -> tuple[bool, bool]:
        charging = self.is_charging_locked()
        previous = self.was_charging
        if self.ready:
            self.was_charging = charging
        return previous, charging

    def handle_charging_transition(self, previous: bool, charging: bool) -> None:
        if not self.ready or charging == previous:
            return

        if charging:
            LOG.info(
                "%s charging started (candidate=%s power=%.1f W)",
                CAR_NAME,
                self.state[ACTIVE_CANDIDATE_TOPIC],
                self.ev_power_w if self.ev_power_w is not None else -1.0,
            )
            self.events.put("charging_started")
        else:
            LOG.info("%s charging stopped", CAR_NAME)
            self.events.put("charging_stopped")

    def on_message(self, client, userdata, msg):
        payload = msg.payload.decode("utf-8", errors="replace").strip()

        if msg.topic == EVENT_TOPIC:
            if payload == "arrived" and self.ready:
                LOG.info("Arrival event received")
                self.events.put("arrived")
            return

        if msg.topic == EV_METER_TOPIC:
            try:
                data = json.loads(payload)
                value = data.get("total_act_power")
                power_w = float(value) if value is not None else None
            except (ValueError, TypeError, json.JSONDecodeError):
                LOG.warning("Invalid EV meter payload")
                return

            with self.state_lock:
                self.ev_power_w = power_w
                previous, charging = self.update_charging_transition_locked()
            self.handle_charging_transition(previous, charging)
            return

        if msg.topic not in self.state:
            return

        with self.state_lock:
            self.state[msg.topic] = payload
            previous, charging = self.update_charging_transition_locked()
        self.handle_charging_transition(previous, charging)

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

    def fetch_and_publish(self, reason: str) -> bool:
        try:
            if self.vehicle is None:
                self.connect_nissan()

            status = self.nissan.battery_status(self.vehicle["vin"])
            soc = status.get("batteryLevel")
            if soc is None:
                raise RuntimeError("batteryLevel missing from Nissan response")

            result = self.mqtt.publish(SOC_TOPIC, str(soc), qos=1, retain=True)
            result.wait_for_publish(timeout=10)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"MQTT publish failed: {result.rc}")

            updated = self.nissan_timestamp(status)
            if updated:
                LOG.info("SoC=%s%% Nissan_updated=%s reason=%s", soc, updated, reason)
            else:
                LOG.info("SoC=%s%% reason=%s", soc, reason)
            return True
        except Exception:
            LOG.exception("SoC fetch failed, reason=%s", reason)
            return False

    def run(self) -> None:
        self.mqtt.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        self.mqtt.loop_start()

        try:
            time.sleep(INITIAL_MQTT_SYNC_SECONDS)
            with self.state_lock:
                self.was_charging = self.is_charging_locked()
                self.ready = True
                charging = self.was_charging
                candidate = self.state[ACTIVE_CANDIDATE_TOPIC]
                selected_car = self.selected_car_locked()
                power_w = self.ev_power_w

            LOG.info(
                "Initial MQTT state synced, charging=%s candidate=%s car=%s power=%s",
                "yes" if charging else "no",
                candidate or "none",
                selected_car or "none",
                f"{power_w:.1f} W" if power_w is not None else "unknown",
            )

            self.fetch_and_publish("startup")

            while True:
                interval = (
                    CHARGING_POLL_SECONDS if self.is_charging() else HOURLY_POLL_SECONDS
                )

                try:
                    reason = self.events.get(timeout=interval)
                except queue.Empty:
                    reason = "scheduled_charging" if self.is_charging() else "scheduled_hourly"

                if reason == "charging_stopped":
                    continue

                # Collapse duplicate immediate triggers already waiting in the queue.
                while True:
                    try:
                        pending = self.events.get_nowait()
                    except queue.Empty:
                        break
                    if pending != "charging_stopped":
                        reason = f"{reason}+{pending}"

                self.fetch_and_publish(reason)
        finally:
            self.mqtt.loop_stop()
            self.mqtt.disconnect()


def main() -> None:
    App().run()


if __name__ == "__main__":
    main()
