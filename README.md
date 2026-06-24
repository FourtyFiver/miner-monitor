# Miner Monitor

cgminer-kompatible ASIC-Miner über MQTT in Home Assistant einbinden.

**Unterstützt:** Antminer Z9 Mini, iPollo V1 Mini Classic, und alle Miner mit cgminer-API (Port 4028).

## Features

- ✅ **Kein SSH, kein Web-Scraping** — nur TCP-Port 4028
- ✅ **MQTT Auto-Discovery** — Sensoren erscheinen automatisch in HA
- ✅ **Docker** — ein Befehl, läuft
- ✅ **YAML-Konfiguration** — Miner einfach hinzufügen/entfernen
- ✅ **Selbstheilend** — Container restartet bei Fehlern automatisch

## Sensoren pro Miner

| Sensor | Beschreibung | Z9 Mini | iPollo V1 |
|--------|-------------|---------|-----------|
| Hashrate | Aktuelle Hashrate in KH/s | ✅ | ✅ |
| Accepted Shares | Akzeptierte Shares (total) | ✅ | ✅ |
| Rejected Shares | Abgelehnte Shares (total) | ✅ | ✅ |
| Hardware Errors | Hardware-Fehler (total) | ✅ | ✅ |
| Temperature | Chip-Temperatur Ø | ✅ | ❌ |
| Board Temperature | Board-Temperatur max | ✅ | ❌ |
| Fan Speed | Lüfterdrehzahl RPM | ✅ | ❌ |
| Online | Erreichbarkeit (binär) | ✅ | ✅ |

## Quick Start

### 1. Konfiguration erstellen

```bash
cp .env.example .env
cp miners.example.yaml miners.yaml
```

`.env` mit deinen MQTT-Daten ausfüllen:

```env
MQTT_HOST=192.168.1.100
MQTT_PORT=1883
MQTT_USER=dein_user
MQTT_PASSWORD=dein_passwort
```

`miners.yaml` mit deinen Minern anpassen:

```yaml
miners:
  - name: z9mini_1
    host: 10.50.13.202
    port: 4028
```

### 2. Starten

```bash
docker compose up -d
```

### 3. Logs prüfen

```bash
docker compose logs -f
```

### 4. In HA

Nach ~30 Sekunden erscheinen die Sensoren automatisch unter **Einstellungen → Geräte & Dienste → MQTT**. Einfach übernehmen.

## Projektstruktur

```
miner-monitor/
├── Dockerfile              # Python-Container
├── docker-compose.yml      # Service-Definition
├── .env.example            # MQTT-Vorlage (ohne Werte)
├── miners.example.yaml     # Miner-Vorlage (mit Beispiel-IPs)
├── .gitignore              # .env + miners.yaml ausgeschlossen
├── requirements.txt        # Python-Abhängigkeiten
├── README.md               # Diese Datei
└── monitor.py              # Hauptskript
```

## Lizenz

MIT
