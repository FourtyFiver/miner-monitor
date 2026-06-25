#!/usr/bin/env python3
"""
miner-monitor — cgminer-compatible ASIC miner monitoring via MQTT + HA Auto-Discovery.

Polls miner API (port 4028) every N seconds and publishes sensor data to MQTT.
Home Assistant auto-discovers all sensors via MQTT Discovery.

Supports:
  - Antminer Z9 Mini (hashrate, temp, fan, shares, pools)
  - iPollo V1 Mini Classic (hashrate, shares, pools — no temp/fan via cgminer API)
  - Any cgminer/bmminer-compatible miner

Configuration via miners.yaml + .env (see .env.example + miners.example.yaml).
"""

import json
import logging
import os
import socket
import struct
import sys
import time
import signal
from pathlib import Path
from typing import Any

import urllib.request
import urllib.error

import paho.mqtt.client as mqtt
import yaml

# ── Config ──────────────────────────────────────────────────────────────────

CONFIG_DIR = Path("/config")
MINERS_FILE = CONFIG_DIR / "miners.yaml"
ENV_FILE = CONFIG_DIR / ".env"

# Fallback: look next to script
if not MINERS_FILE.exists():
    MINERS_FILE = Path(__file__).parent / "miners.yaml"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("miner-monitor")

# ── cgminer API ─────────────────────────────────────────────────────────────

CGMINER_MAGIC = struct.pack(b"4s", b"\x00\x00\x00\x00")


def cgminer_request(host: str, port: int, command: str, timeout: float = 5.0) -> dict | None:
    """Send a command to a cgminer/ccminer-compatible miner.

    Supports three wire formats:
      - cgminer: 4-byte magic + 4-byte length + JSON (port 4028)
      - ccminer: raw text command + null byte, response is KEY=VALUE; pairs
    """
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.settimeout(timeout)

        # Try ccminer raw text format first (fails fast if wrong)
        resp = b""
        try:
            sock.send(command.encode() + b"\x00")
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                resp += chunk
        except socket.timeout:
            pass

        if resp:
            sock.close()
            text = resp.decode().strip().strip("\x00")
            if "=" in text:
                result = {}
                pairs = text.replace("|", ";").split(";")
                for pair in pairs:
                    pair = pair.strip()
                    if "=" in pair:
                        k, v = pair.split("=", 1)
                        result[k.strip()] = v.strip()
                return result
            if text.startswith("{"):
                return json.loads(text)
            return None

        # No response from ccminer format → try cgminer JSON format
        sock.close()
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.settimeout(timeout)
        payload = json.dumps({"command": command}) + "\n"
        sock.send(CGMINER_MAGIC + struct.pack(b"<I", len(payload)) + payload.encode())
        raw = sock.recv(4)
        if len(raw) < 4:
            sock.close()
            return None
        if raw == CGMINER_MAGIC:
            raw_len = sock.recv(4)
        else:
            raw_len = raw
        if len(raw_len) < 4:
            sock.close()
            return None
        resp_len = struct.unpack(b"<I", raw_len)[0]
        resp = b""
        while len(resp) < resp_len:
            chunk = sock.recv(resp_len - len(resp))
            if not chunk:
                break
            resp += chunk
        sock.close()
        return json.loads(resp.decode().strip("\x00"))
    except (socket.timeout, ConnectionRefusedError, OSError, json.JSONDecodeError) as exc:
        log.debug("cgminer request to %s:%s (%s) failed: %s", host, port, command, exc)
        return None


# ── XMRig HTTP API ──────────────────────────────────────────────────────────


def xmrig_http_request(
    host: str, port: int, endpoint: str, token: str, timeout: float = 5.0
) -> dict | None:
    """Send an HTTP GET request to the XMRig API with Bearer token auth."""
    url = f"http://{host}:{port}/1/{endpoint}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, OSError, json.JSONDecodeError, TimeoutError) as exc:
        log.debug("XMRig HTTP request to %s:%s/%s failed: %s", host, port, endpoint, exc)
        return None


def extract_xmrig_data(host: str, port: int, token: str) -> dict | None:
    """Poll an XMRig miner via its HTTP API and return a flat dict of sensor values."""
    summary = xmrig_http_request(host, port, "summary", token)
    if not summary or summary.get("status") == "error":
        return None

    data: dict[str, Any] = {
        "online": True,
        "paused": summary.get("paused", False),
    }

    # Hashrate: hashrate.total is a list of 3 values [current, best, total]
    hr_total = summary.get("hashrate", {}).get("total")
    if hr_total and isinstance(hr_total, list) and len(hr_total) > 0:
        data["hashrate"] = float(hr_total[0])
    else:
        # Fallback: use the sum of all thread hashrates
        threads = summary.get("mining", {}).get("threads", [])
        total_hr = sum(t.get("current", 0) for t in threads) if threads else 0
        data["hashrate"] = float(total_hr)
    data["hashrate_unit"] = "H/s"

    # Peak hashrate
    hr_highest = summary.get("hashrate", {}).get("highest")
    if hr_highest is not None:
        data["hashrate_highest"] = float(hr_highest)

    # Shares
    results = summary.get("results", {})
    data["accepted"] = int(results.get("shares_good", 0))
    data["rejected"] = int(results.get("shares_bad", 0))
    # shares_total includes good + bad, rejected = total - good as alternative
    if "shares_total" in results and "shares_good" in results:
        rej = max(
            int(results.get("shares_bad", 0)),
            int(results["shares_total"]) - int(results["shares_good"]),
        )
        data["rejected"] = rej

    data["hw_errors"] = 0

    # Connection / pool info
    conn = summary.get("connection", {})
    if conn:
        accepted_pool = conn.get("accepted")
        rejected_pool = conn.get("rejected")
        if accepted_pool is not None:
            data["pool_accepted"] = int(accepted_pool)
        if rejected_pool is not None:
            data["pool_rejected"] = int(rejected_pool)
        pool_url = conn.get("pool", "")
        if pool_url:
            data["pool_url"] = str(pool_url)
        ping = conn.get("ping")
        if ping is not None:
            data["pool_ping"] = int(ping)

    # Uptime
    uptime = summary.get("uptime")
    if uptime is not None:
        data["uptime"] = int(uptime)

    # CPU info
    cpu = summary.get("cpu", {})
    if cpu:
        brand = cpu.get("brand", "")
        if brand:
            data["cpu_brand"] = str(brand)
        threads_count = cpu.get("threads")
        if threads_count is not None:
            data["cpu_threads"] = int(threads_count)

    # Resources
    resources = summary.get("resources", {})
    if resources:
        load_avg = resources.get("load_average")
        if load_avg and isinstance(load_avg, list) and len(load_avg) > 0:
            data["load_average"] = float(load_avg[0])

    return data


# ── Miner data extraction ────────────────────────────────────────────────────


def extract_miner_data(host: str, port: int) -> dict | None:
    """Poll a miner and return a flat dict of sensor values, or None if offline."""
    summary = cgminer_request(host, port, "summary")
    if not summary:
        return None

    data: dict[str, Any] = {
        "online": True,
    }

    # ccminer text format: flat KEY=VALUE dict
    # Z9 Mini liefert: STATUS=S,...,Description=...|SUMMARY,Elapsed=5044,GHS 5s=13.48,...
    # Nach |->; Split wird SUMMARY,Elapsed=5044 zum Key — die Komma-Felder müssen extra geparst werden
    # Fallback: Komma-getrennte Felder im SUMMARY-Eintrag parsen
    # Wird IMMER ausgeführt, nicht nur wenn has_hashrate_key False ist
    for k, v in list(summary.items()):
        if k.startswith("SUMMARY") and "," in k:
            rest = v
            pairs2 = rest.split(",")
            for pair in pairs2:
                if "=" in pair:
                    sk, sv = pair.split("=", 1)
                    summary[sk.strip()] = sv.strip()

    has_hashrate_key = any(
        k.startswith(prefix) for k in summary for prefix in ["KHS", "GHS", "MHS"]
    )

    if has_hashrate_key:
        for key_pattern, unit in [("KHS", "KH/s"), ("GHS", "GH/s"), ("MHS", "MH/s")]:
            matching_keys = [k for k in summary if k.startswith(key_pattern)]
            if matching_keys:
                raw_val = summary.get(matching_keys[0], "0")
                # Suffix entfernen: 134.9204M, 12.46G, 500K
                raw_val = raw_val.strip().upper()
                multiplier = 1.0
                for suffix, factor in [("P", 1e15), ("T", 1e12), ("G", 1e9), ("M", 1e6), ("K", 1e3)]:
                    if raw_val.endswith(suffix):
                        # Nur multiplizieren wenn die Einheit nicht schon passt
                        # z.B. MHS av=136.5566M → M suffix, aber Einheit ist MH/s → Wert ist 136.5566
                        if not (unit.startswith(suffix[0]) and suffix in ("M", "K", "G")):
                            multiplier = factor
                        raw_val = raw_val[:-1]
                        break
                try:
                    val = float(raw_val) * multiplier
                except ValueError:
                    val = 0.0
                data["hashrate"] = val
                data["hashrate_unit"] = unit
                # Z9 Mini: GH/s → KH/s für HA-Kompatibilität
                if unit == "GH/s":
                    data["hashrate"] = val * 1000
                    data["hashrate_unit"] = "KH/s"
                break
        data["accepted"] = int(summary.get("ACC", summary.get("Accepted", 0)))
        data["rejected"] = int(summary.get("REJ", summary.get("Rejected", 0)))
        data["hw_errors"] = int(summary.get("Hardware Errors", summary.get("HW", 0)))
        data["stale"] = int(summary.get("Stale", 0))
        data["elapsed"] = int(summary.get("UPTIME", 0))
        data["pool_alive"] = int(summary.get("POOLS", 0)) > 0

        # Auch bei ccminer-Format: Stats für Temp/Fan abfragen
        stats = cgminer_request(host, port, "stats")
        if stats:
            # Stats hat auch Komma-getrennte Felder (STATS,ID=0,fan1=4200,...)
            for k, v in list(stats.items()):
                if "," in v and "=" in v:
                    pairs2 = v.split(",")
                    for pair in pairs2:
                        if "=" in pair:
                            sk, sv = pair.split("=", 1)
                            stats[sk.strip()] = sv.strip()
            for k, v in stats.items():
                if k.startswith("temp") and not k.startswith("temp2"):
                    try:
                        data.setdefault("temp_avg", 0)
                        data.setdefault("temp_count", 0)
                        data["temp_avg"] = data.get("temp_avg", 0) + int(v)
                        data["temp_count"] = data.get("temp_count", 0) + 1
                        data["temp_max"] = max(data.get("temp_max", 0), int(v))
                    except (ValueError, TypeError):
                        pass
                if k.startswith("temp2_"):
                    try:
                        data.setdefault("temp_board_avg", 0)
                        data.setdefault("temp_board_count", 0)
                        data["temp_board_avg"] = data.get("temp_board_avg", 0) + int(v)
                        data["temp_board_count"] = data.get("temp_board_count", 0) + 1
                        data["temp_board_max"] = max(data.get("temp_board_max", 0), int(v))
                    except (ValueError, TypeError):
                        pass
                if k.startswith("fan") and v and str(v).isdigit() and int(v) > 0:
                    data["fan_speed"] = int(v)
            if data.get("temp_count", 0) > 0:
                data["temp_avg"] = round(data["temp_avg"] / data["temp_count"], 1)
            if data.get("temp_board_count", 0) > 0:
                data["temp_board_avg"] = round(data["temp_board_avg"] / data["temp_board_count"], 1)

        return data

    # cgminer JSON format
    if "SUMMARY" not in summary:
        return None

    s = summary["SUMMARY"][0]
    data["elapsed"] = s.get("Elapsed", 0)

    # Hashrate — Z9 reports GHS, iPollo reports MHS
    for key in ("GHS 5s", "GHS av", "MHS av", "MHS 1m", "MHS 5m", "MHS 15m"):
        if key in s:
            data["hashrate"] = float(s[key])
            hr_unit = "GH/s" if key.startswith("GHS") else "MH/s"
            # Z9 Mini: GH/s → KH/s für HA-Kompatibilität
            if hr_unit == "GH/s":
                data["hashrate"] = float(s[key]) * 1000
                data["hashrate_unit"] = "KH/s"
            else:
                data["hashrate_unit"] = hr_unit
            break

    data["accepted"] = s.get("Accepted", 0)
    data["rejected"] = s.get("Rejected", 0)
    data["stale"] = s.get("Stale", 0)
    data["hw_errors"] = s.get("Hardware Errors", 0)
    data["utility"] = s.get("Utility", 0.0)

    # Pool info
    pools = cgminer_request(host, port, "pools")
    if pools and "POOLS" in pools:
        alive_pools = [p for p in pools["POOLS"] if p.get("Status") == "Alive"]
        data["pool_alive"] = len(alive_pools) > 0
        data["pool_url"] = alive_pools[0].get("URL", "") if alive_pools else ""
        data["pool_user"] = alive_pools[0].get("User", "") if alive_pools else ""

    # Stats — contains temp/fan for Z9, not for iPollo
    stats = cgminer_request(host, port, "stats")
    if stats and "STATS" in stats:
        for entry in stats["STATS"]:
            if isinstance(entry, dict):
                # Z9 Mini: temp1/2/3, temp2_1/2/3 (board), fan1
                temps = []
                board_temps = []
                for k, v in entry.items():
                    if k.startswith("temp") and not k.startswith("temp2"):
                        try:
                            temps.append(int(v))
                        except (ValueError, TypeError):
                            pass
                    if k.startswith("temp2_"):
                        try:
                            board_temps.append(int(v))
                        except (ValueError, TypeError):
                            pass
                if temps:
                    data["temp_avg"] = round(sum(temps) / len(temps), 1)
                    data["temp_max"] = max(temps)
                if board_temps:
                    data["temp_board_avg"] = round(sum(board_temps) / len(board_temps), 1)
                    data["temp_board_max"] = max(board_temps)

                for k, v in entry.items():
                    if k.startswith("fan") and v and int(v) > 0:
                        data["fan_speed"] = int(v)
                        break

                # Chain status (Z9)
                chains = []
                for k, v in entry.items():
                    if k.startswith("chain_acs"):
                        chains.append(str(v))
                if chains:
                    data["chains"] = chains

    return data


# ── MQTT ────────────────────────────────────────────────────────────────────


class MQTTPublisher:
    def __init__(self, host: str, port: int, user: str, password: str, prefix: str):
        self.prefix = prefix
        self.client = mqtt.Client(client_id="miner-monitor", protocol=mqtt.MQTTv311)
        self.client.username_pw_set(user, password)
        self.client.will_set(
            f"{prefix}/status", payload="offline", qos=1, retain=True
        )
        self.client.on_connect = self._on_connect
        self._connected = False
        self._last_connect_attempt = 0.0

        try:
            self.client.connect(host, port, keepalive=60)
            self.client.loop_start()
            # Wait briefly for connection
            for _ in range(20):
                if self._connected:
                    break
                time.sleep(0.1)
        except Exception as exc:
            log.warning("MQTT connection failed: %s (will retry)", exc)

    def _on_connect(self, _client, _userdata, _flags, rc):
        if rc == 0:
            self._connected = True
            log.info("MQTT connected")
            self.client.publish(f"{self.prefix}/status", "online", qos=1, retain=True)
        else:
            log.warning("MQTT connection failed with code %d", rc)

    def publish_discovery(self, miner_name: str, data: dict):
        """Publish MQTT discovery configs for Home Assistant."""
        base = f"{self.prefix}/sensor/{miner_name}"

        sensors = {
            "hashrate": {
                "name": f"{miner_name} Hashrate",
                "unit_of_measurement": data.get("hashrate_unit", "KH/s"),
                "icon": "mdi:speedometer",
                "state_class": "measurement",
            },
            "accepted": {
                "name": f"{miner_name} Accepted Shares",
                "icon": "mdi:check-circle",
                "state_class": "total_increasing",
            },
            "rejected": {
                "name": f"{miner_name} Rejected Shares",
                "icon": "mdi:close-circle",
                "state_class": "total_increasing",
            },
            "hw_errors": {
                "name": f"{miner_name} Hardware Errors",
                "icon": "mdi:alert-circle",
                "state_class": "total_increasing",
            },
        }

        if "temp_avg" in data:
            sensors["temp_avg"] = {
                "name": f"{miner_name} Temperature",
                "unit_of_measurement": "°C",
                "icon": "mdi:thermometer",
                "device_class": "temperature",
                "state_class": "measurement",
            }
        if "temp_board_max" in data:
            sensors["temp_board"] = {
                "name": f"{miner_name} Board Temperature",
                "unit_of_measurement": "°C",
                "icon": "mdi:thermometer",
                "device_class": "temperature",
                "state_class": "measurement",
            }
        if "fan_speed" in data:
            sensors["fan_speed"] = {
                "name": f"{miner_name} Fan Speed",
                "unit_of_measurement": "RPM",
                "icon": "mdi:fan",
                "state_class": "measurement",
            }

        # XMRig-specific sensors (conditional discovery)
        if "hashrate_highest" in data:
            sensors["hashrate_highest"] = {
                "name": f"{miner_name} Peak Hashrate",
                "unit_of_measurement": "H/s",
                "icon": "mdi:speedometer",
                "state_class": "measurement",
            }
        if "pool_ping" in data:
            sensors["pool_ping"] = {
                "name": f"{miner_name} Pool Ping",
                "unit_of_measurement": "ms",
                "icon": "mdi:lan",
                "state_class": "measurement",
            }
        if "load_average" in data:
            sensors["load_average"] = {
                "name": f"{miner_name} Load Average",
                "icon": "mdi:chart-line",
                "state_class": "measurement",
            }
        if "uptime" in data:
            sensors["uptime"] = {
                "name": f"{miner_name} Uptime",
                "unit_of_measurement": "s",
                "icon": "mdi:clock",
                "state_class": "measurement",
            }

        # XMRig text sensors (cpu_brand, pool_url) — published separately below
        text_sensors: dict[str, dict] = {}
        if "cpu_brand" in data:
            text_sensors["cpu_brand"] = {
                "name": f"{miner_name} CPU",
                "icon": "mdi:cpu",
                "device_class": "",
            }
        if "pool_url" in data:
            text_sensors["pool_url"] = {
                "name": f"{miner_name} Pool URL",
                "icon": "mdi:server-network",
                "device_class": "",
            }

        # Binary sensor for online status
        binary_topic = f"{self.prefix}/binary_sensor/{miner_name}/online/config"
        binary_config = {
            "name": f"{miner_name} Online",
            "unique_id": f"miner_{miner_name}_online",
            "state_topic": f"{base}/online/state",
            "payload_on": "True",
            "payload_off": "False",
            "device_class": "connectivity",
            "icon": "mdi:server",
            "device": {
                "identifiers": [f"miner_{miner_name}"],
                "name": f"Miner {miner_name}",
                "model": "ASIC Miner",
                "manufacturer": "cgminer",
            },
        }
        self.client.publish(binary_topic, json.dumps(binary_config), qos=1, retain=True)

        for sensor_key, cfg in sensors.items():
            config = {
                **cfg,
                "unique_id": f"miner_{miner_name}_{sensor_key}",
                "state_topic": f"{base}/{sensor_key}/state",
                "device": {
                    "identifiers": [f"miner_{miner_name}"],
                    "name": f"Miner {miner_name}",
                },
            }
            topic = f"{self.prefix}/sensor/{miner_name}/{sensor_key}/config"
            self.client.publish(topic, json.dumps(config), qos=1, retain=True)

        # Publish XMRig text sensors (cpu_brand, pool_url) via homeassistant/text_sensor/
        for tex_key, t_cfg in text_sensors.items():
            text_config = {
                **t_cfg,
                "unique_id": f"miner_{miner_name}_{tex_key}",
                "state_topic": f"{base}/{tex_key}/state",
                "device": {
                    "identifiers": [f"miner_{miner_name}"],
                    "name": f"Miner {miner_name}",
                },
            }
            text_topic = f"{self.prefix}/text_sensor/{miner_name}/{tex_key}/config"
            self.client.publish(text_topic, json.dumps(text_config), qos=1, retain=True)

    def publish_state(self, miner_name: str, data: dict):
        """Publish current sensor values."""
        base = f"{self.prefix}/sensor/{miner_name}"

        # Online status
        self.client.publish(
            f"{base}/online/state", str(data.get("online", False)), qos=1, retain=True
        )

        # Hashrate — normalize to KH/s for consistency
        hr = data.get("hashrate", 0)
        hr_unit = data.get("hashrate_unit", "KH/s")
        if hr_unit == "MH/s":
            hr = hr * 1000  # MH/s → KH/s
        elif hr_unit == "GH/s":
            hr = hr * 1000 * 1000  # GH/s → KH/s
        self.client.publish(f"{base}/hashrate/state", f"{hr:.2f}", qos=1, retain=True)

        self.client.publish(f"{base}/accepted/state", str(data.get("accepted", 0)), qos=1, retain=True)
        self.client.publish(f"{base}/rejected/state", str(data.get("rejected", 0)), qos=1, retain=True)
        self.client.publish(f"{base}/hw_errors/state", str(data.get("hw_errors", 0)), qos=1, retain=True)

        if "temp_avg" in data:
            self.client.publish(f"{base}/temp_avg/state", str(data["temp_avg"]), qos=1, retain=True)
        if "temp_board_max" in data:
            self.client.publish(f"{base}/temp_board/state", str(data["temp_board_max"]), qos=1, retain=True)
        if "fan_speed" in data:
            self.client.publish(f"{base}/fan_speed/state", str(data["fan_speed"]), qos=1, retain=True)

        # XMRig-specific sensor states (conditional)
        if "hashrate_highest" in data:
            self.client.publish(
                f"{base}/hashrate_highest/state", str(data["hashrate_highest"]), qos=1, retain=True
            )
        if "pool_ping" in data:
            self.client.publish(f"{base}/pool_ping/state", str(data["pool_ping"]), qos=1, retain=True)
        if "load_average" in data:
            self.client.publish(
                f"{base}/load_average/state", str(data["load_average"]), qos=1, retain=True
            )
        if "uptime" in data:
            self.client.publish(f"{base}/uptime/state", str(data["uptime"]), qos=1, retain=True)
        if "cpu_brand" in data:
            self.client.publish(
                f"{base}/cpu_brand/state", str(data["cpu_brand"]), qos=1, retain=True
            )
        if "pool_url" in data:
            self.client.publish(f"{base}/pool_url/state", str(data["pool_url"]), qos=1, retain=True)

    def publish_availability(self, miner_name: str, online: bool):
        """Publish just the online/offline status."""
        base = f"{self.prefix}/sensor/{miner_name}"
        self.client.publish(f"{base}/online/state", str(online), qos=1, retain=True)

    def publish_offline_zero(self, miner_name: str):
        """Zero out all numeric sensors when a miner goes offline.

        Resets hashrate, accepted, rejected, hw_errors, temp, fan,
        and XMRig-specific sensors (hashrate_highest, pool_ping,
        load_average, uptime) to 0 so HA doesn't show stale values.
        Text sensors (cpu_brand, pool_url) are cleared to empty string.
        Uses retain=True so the zero persists in the MQTT broker
        (HA reads retained state on reconnect/restart).
        """
        base = f"{self.prefix}/sensor/{miner_name}"
        numeric_sensors = [
            "hashrate", "accepted", "rejected", "hw_errors",
            "temp_avg", "temp_board", "fan_speed",
            "hashrate_highest", "pool_ping", "load_average", "uptime",
        ]
        for sensor in numeric_sensors:
            self.client.publish(f"{base}/{sensor}/state", "0", qos=1, retain=True)
        # Text sensors leeren
        for sensor in ["cpu_brand", "pool_url"]:
            self.client.publish(f"{base}/{sensor}/state", "", qos=1, retain=True)

    def disconnect(self):
        self.client.publish(f"{self.prefix}/status", "offline", qos=1, retain=True)
        self.client.disconnect()
        self.client.loop_stop()


# ── Main loop ───────────────────────────────────────────────────────────────


def load_miners() -> list[dict]:
    """Load miner configuration from YAML."""
    if not MINERS_FILE.exists():
        log.error("miners.yaml not found at %s", MINERS_FILE)
        log.error("Create it from miners.example.yaml")
        sys.exit(1)

    with open(MINERS_FILE) as f:
        config = yaml.safe_load(f)

    miners = config.get("miners", [])
    if not miners:
        log.error("No miners defined in %s", MINERS_FILE)
        sys.exit(1)

    log.info("Loaded %d miner(s) from %s", len(miners), MINERS_FILE)
    for m in miners:
        miner_type = m.get("type", "cgminer")
        has_token = bool(m.get("api_token"))
        log.info(
            "  - %s (%s:%s) type=%s api_token=%s",
            m["name"], m["host"], m.get("port", 4028),
            miner_type, "[set]" if has_token else "[none]",
        )
    return miners


def load_env() -> dict:
    """Load MQTT settings from environment or .env file."""
    mqtt_host = os.getenv("MQTT_HOST", "")
    mqtt_port = int(os.getenv("MQTT_PORT", "1883"))
    mqtt_user = os.getenv("MQTT_USER", "")
    mqtt_password = os.getenv("MQTT_PASSWORD", "")
    mqtt_prefix = os.getenv("MQTT_PREFIX", "homeassistant")
    poll_interval = int(os.getenv("POLL_INTERVAL", "60"))

    # Try .env file as fallback
    if not mqtt_host and ENV_FILE.exists():
        with open(ENV_FILE) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip("\"'")
                    if k == "MQTT_HOST" and not mqtt_host:
                        mqtt_host = v
                    elif k == "MQTT_PORT" and mqtt_port == 1883:
                        mqtt_port = int(v)
                    elif k == "MQTT_USER" and not mqtt_user:
                        mqtt_user = v
                    elif k == "MQTT_PASSWORD" and not mqtt_password:
                        mqtt_password = v
                    elif k == "MQTT_PREFIX" and mqtt_prefix == "homeassistant":
                        mqtt_prefix = v
                    elif k == "POLL_INTERVAL" and poll_interval == 60:
                        poll_interval = int(v)

    if not mqtt_host:
        log.error("MQTT_HOST not set! Create .env or set environment variables.")
        sys.exit(1)

    return {
        "host": mqtt_host,
        "port": mqtt_port,
        "user": mqtt_user,
        "password": mqtt_password,
        "prefix": mqtt_prefix,
        "poll_interval": poll_interval,
    }


def main():
    log.info("╔══════════════════════════════════════════╗")
    log.info("║  Miner Monitor — cgminer → MQTT Bridge  ║")
    log.info("╚══════════════════════════════════════════╝")

    mqtt_cfg = load_env()
    miners = load_miners()

    publisher = MQTTPublisher(
        host=mqtt_cfg["host"],
        port=mqtt_cfg["port"],
        user=mqtt_cfg["user"],
        password=mqtt_cfg["password"],
        prefix=mqtt_cfg["prefix"],
    )

    if not publisher._connected:
        log.warning("Starting without MQTT connection — will retry...")

    # Initial discovery
    discovery_done = set()
    poll_interval = mqtt_cfg["poll_interval"]

    # Handle graceful shutdown
    shutdown = False

    def _signal_handler(sig, frame):
        nonlocal shutdown
        log.info("Shutting down...")
        shutdown = True

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    while not shutdown:
        for miner in miners:
            name = miner["name"]
            host = miner["host"]
            port = miner.get("port", 4028)
            miner_type = miner.get("type", "cgminer")

            # Dispatch to the right data extractor based on miner type
            if miner_type == "xmrig":
                token = miner.get("api_token", "")
                data = extract_xmrig_data(host, port, token)
            else:
                data = extract_miner_data(host, port)

            if data is None:
                if name in discovery_done:
                    publisher.publish_availability(name, False)
                    publisher.publish_offline_zero(name)
                log.warning("⛔ %s (%s:%s) — OFFLINE", name, host, port)
                continue

            # First discovery for this miner
            if name not in discovery_done:
                publisher.publish_discovery(name, data)
                discovery_done.add(name)
                log.info("🔍 %s — Discovery published", name)

            # Publish state
            publisher.publish_state(name, data)

            # Log summary
            hr = data.get("hashrate", 0)
            hr_unit = data.get("hashrate_unit", "KH/s")
            temp = data.get("temp_avg", "N/A")
            fan = data.get("fan_speed", "N/A")

            if miner_type == "xmrig":
                load_avg = data.get("load_average", "N/A")
                uptime = data.get("uptime", "N/A")
                log.info(
                    "✅ %s — %s %s | Load: %s | Uptime: %ss | Acc: %s | Rej: %s",
                    name, hr, hr_unit, load_avg, uptime,
                    data.get("accepted", 0), data.get("rejected", 0),
                )
            else:
                log.info(
                    "✅ %s — %s %s | Temp: %s°C | Fan: %s RPM | Acc: %s | Rej: %s",
                    name, hr, hr_unit, temp, fan,
                    data.get("accepted", 0), data.get("rejected", 0),
                )

        if not shutdown:
            time.sleep(poll_interval)

    publisher.disconnect()
    log.info("Bye.")


if __name__ == "__main__":
    main()
