# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

cgminer-to-MQTT bridge — polls ASIC miners (Antminer Z9 Mini, iPollo V1 Mini Classic, or any cgminer-compatible device) via the cgminer JSON-RPC API on TCP port 4028 and publishes sensor data to MQTT for Home Assistant auto-discovery. Single Python script deployed via Docker.

## Commands

```bash
# Run locally (no Docker) — set env vars or create .env + miners.yaml in CWD
pip install -r requirements.txt
python3 monitor.py

# Build and run via Docker
docker compose up -d --build

# View logs
docker compose logs -f

# Stop
docker compose down
```

## Architecture

### Entry point: `monitor.py`

A single ~460-line Python script. Startup flow:

1. `load_env()` — reads MQTT credentials. Environment variables take precedence; `.env` file is a fallback. Exits if `MQTT_HOST` is not set.
2. `load_miners()` — reads `miners.yaml`. Exits if no miners defined.
3. `MQTTPublisher.__init__()` — connects to MQTT, sets a Last Will message (`<prefix>/status → "offline"`), starts paho's background network thread via `loop_start()`.
4. **Main loop**: polls each miner with `extract_miner_data()`, publishes HA discovery configs on first successful poll, then publishes state values each iteration. Runs until SIGTERM/SIGINT.

### cgminer wire protocol

The function `cgminer_request()` implements the cgminer RPC wire format: 4-byte zero magic (`\x00\x00\x00\x00`) + 4-byte little-endian payload length + JSON payload. Response uses the same framing. The script polls three API commands per miner per cycle: `summary`, `pools`, `stats`.

### MQTT topic layout

```
<prefix>/status                          — "online" / "offline" (LWT)
<prefix>/sensor/<name>/<key>/config      — HA discovery payload (retained)
<prefix>/sensor/<name>/<key>/state       — current value
<prefix>/binary_sensor/<name>/online/config — online/offline binary sensor (retained)
```

The default prefix is `homeassistant` (matches HA's default MQTT discovery prefix). Change via `MQTT_PREFIX`.

### Miner data normalization

- Hashrate: Z9 reports in `GHS av`/`GHS 5s`, iPollo reports in `MHS av`/`MHS 5m`/`MHS 15m`. All are converted to KH/s when publishing state. The discovery config is tagged with the raw unit for HA display.
- Temperature/fan: extracted from `stats` response. Z9 exposes `temp1`/`temp2`/`temp3` (chip) and `temp2_1`/`temp2_2`/`temp2_3` (board). iPollo has no temp/fan via cgminer API — those sensors are conditionally registered only when data is present.

### Docker networking

The compose file uses `network_mode: host` so the container can reach miners on the local network without Docker's NAT. Config files are mounted read-only into `/config/`.

### Config file resolution

When running in Docker, config lives at `/config/miners.yaml` and `/config/.env`. When running locally (no Docker), the script falls back to files next to `monitor.py` in the working directory.