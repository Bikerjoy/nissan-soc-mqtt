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
CONTROL_REASON_TOPIC = "home/ev/control/auto_charge_reason"
GARAGE_STATUS_TOPIC = "home/ev/garage/status"
CARPORT_STATUS_TOPIC = "home/ev/carport/status"

BATTERY_CAPACITY_KWH = 40.0
CHARGE_EFFICIENCY = 0.84
EV_BASELINE_W = 30.0
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
        self.last_meter_time: float | None = None
        self.last_effective_battery_power_w: float | None = None
        self.estimated_soc: float | None = None
        self.control_reason: str | None = None
        self.charger_status: dict[str, str | None] = {
            GARAGE_STATUS_TOPIC: None,
            CARPORT_STATUS_TOPIC: None,
        }
        self.ready = False
        self.events: queue.Queue[str] = queue.Queue()

        try:
            self.mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        except AttributeError:
            self.mqtt = mqtt.Client()

        self.mqtt.on_connect = self.on_connect
        self.mqtt.on_message = self.on_message

    @staticmethod
    def effective_battery_power_w(ev_power_w: float | None) -> float:
        if ev_power_w is None:
            return 0.0
        return max(ev_power_w - EV_BASELINE_W, 0.0) * CHARGE_EFFICIENCY

    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code != 0:
            LOG.error("MQTT connection failed: %s", reason_code)
            return

        LOG.info("Connected to MQTT broker")
        client.subscribe(EV_METER_TOPIC)
        client.subscribe(CONTROL_REASON_TOPIC)
        client.subscribe(GARAGE_STATUS_TOPIC)
        client.subscribe(CARPORT_STATUS_TOPIC)
        client.subscribe(SOC_TOPIC)

    def publish_retained(self, topic: str, payload: str) -> None:
        result = self.mqtt.publish(topic, payload, qos=1, retain=True)
        result.wait_for_publish(timeout=10)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"MQTT publish failed for {topic}: {result.rc}")

    def publish_estimated_soc(self, soc: float) -> None:
        # Do not block the MQTT callback thread waiting for its own PUBACK.
        result = self.mqtt.publish(
            SOC_TOPIC,
            f"{max(0.0, min(100.0, soc)):.2f}",
            qos=1,
            retain=True,
        )
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            LOG.error("MQTT publish failed for %s: %s", SOC_TOPIC, result.rc)

    def integrate_meter_sample(self, power_w: float | None) -> None:
        now = time.monotonic()
        effective_power_w = self.effective_battery_power_w(power_w)
        publish_soc: float | None = None

        with self.state_lock:
            if (
                self.estimated_soc is not None
                and self.last_meter_time is not None
                and self.last_effective_battery_power_w is not None
            ):
                elapsed_hours = max(now - self.last_meter_time, 0.0) / 3600.0
                average_power_w = (
                    self.last_effective_battery_power_w + effective_power_w
                ) / 2.0
                energy_kwh = average_power_w * elapsed_hours / 1000.0
                soc_delta = energy_kwh / BATTERY_CAPACITY_KWH * 100.0
                if soc_delta > 0.0:
                    self.estimated_soc = min(100.0, self.estimated_soc + soc_delta)
                    publish_soc = self.estimated_soc

            self.ev_power_w = power_w
            self.last_meter_time = now
            self.last_effective_battery_power_w = effective_power_w

        if publish_soc is not None:
            self.publish_estimated_soc(publish_soc)

    def on_message(self, client, userdata, msg):
        payload = msg.payload.decode("utf-8", errors="replace").strip()

        if msg.topic == EV_METER_TOPIC:
            try:
                data = json.loads(payload)
                value = data.get("total_act_power")
                power_w = float(value) if value is not None else None
            except (ValueError, TypeError, json.JSONDecodeError):
                LOG.warning("Invalid EV meter payload")
                return
            self.integrate_meter_sample(power_w)
            return

        if msg.topic == SOC_TOPIC:
            # Re-use our retained estimate after a service restart so startup does
            # not require a Nissan API request. Later self-published values are ignored.
            try:
                retained_soc = float(payload)
            except ValueError:
                return
            with self.state_lock:
                if self.estimated_soc is None:
                    self.estimated_soc = max(0.0, min(100.0, retained_soc))
                    self.last_meter_time = time.monotonic()
                    self.last_effective_battery_power_w = self.effective_battery_power_w(
                        self.ev_power_w
                    )
                    LOG.info(
                        "Restored retained SoC estimate %.2f%%",
                        self.estimated_soc,
                    )
            return

        if msg.topic == CONTROL_REASON_TOPIC:
            with self.state_lock:
                previous = self.control_reason
                self.control_reason = payload
                ready = self.ready

            # auto_charge_reason is retained and published continuously. Only the
            # transition into night_reserve_stop is a calibration event.
            if (
                ready
                and not msg.retain
                and payload == "night_reserve_stop"
                and previous != "night_reserve_stop"
            ):
                LOG.info("Home battery SoC reserve stop; polling Nissan")
                self.events.put("soc_reserve_stop")
            return

        if msg.topic in self.charger_status:
            with self.state_lock:
                previous = self.charger_status[msg.topic]
                self.charger_status[msg.topic] = payload
                ready = self.ready

            if not ready or msg.retain or payload == previous:
                return

            location = "garage" if msg.topic == GARAGE_STATUS_TOPIC else "carport"
            if payload == "in_progress":
                LOG.info("Charging session started at %s; polling Nissan", location)
                self.events.put(f"session_started_{location}")
            elif payload == "completed":
                LOG.info("Charging session completed at %s; polling Nissan", location)
                self.events.put(f"session_completed_{location}")

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

    def fetch_and_publish(self, reason: str) -> bool:
        try:
            if self.vehicle is None:
                self.connect_nissan()

            status = self.nissan.battery_status(self.vehicle["vin"])
            soc = status.get("batteryLevel")
            if soc is None:
                raise RuntimeError("batteryLevel missing from Nissan response")

            soc_value = max(0.0, min(100.0, float(soc)))
            plug_status = status.get("plugStatus")
            charge_status = status.get("chargeStatus")
            plug_label = self.plug_label(plug_status)
            charge_label = self.charge_label(charge_status)

            with self.state_lock:
                # Every successful Nissan poll is a fresh calibration point. Meter
                # integration continues from this value and this instant.
                self.estimated_soc = soc_value
                self.last_meter_time = time.monotonic()
                self.last_effective_battery_power_w = self.effective_battery_power_w(
                    self.ev_power_w
                )

            self.publish_retained(SOC_TOPIC, f"{soc_value:.2f}")
            self.publish_retained(CHARGING_TOPIC, charge_label)

            updated = self.nissan_timestamp(status)
            LOG.info(
                "SoC calibrated=%s%% plugged=%s charging=%s Nissan_updated=%s reason=%s",
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
                estimate = self.estimated_soc

            LOG.info(
                "Initial MQTT state synced, power=%s estimated_soc=%s",
                f"{power_w:.1f} W" if power_w is not None else "unknown",
                f"{estimate:.2f}%" if estimate is not None else "unknown",
            )

            # No scheduled Nissan polling. Existing EV-controller session state
            # decides when calibration is useful: session start, reserve stop,
            # and session completion.
            while True:
                reason = self.events.get()

                # Coalesce simultaneous controller events into a single API call.
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
