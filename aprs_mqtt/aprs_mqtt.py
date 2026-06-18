#!/usr/bin/env python3
"""
APRS MQTT Tracker
Connects to APRS-IS, listens for configured callsigns,
and publishes position/telemetry data to MQTT with
Home Assistant auto-discovery support.
"""

import os
import json
import time
import logging
import threading
import signal
import sys
from datetime import datetime, timezone

import aprslib
import paho.mqtt.client as mqtt

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("aprs_mqtt")

# ---------------------------------------------------------------------------
# Configuration from environment (set by run.sh via bashio)
# ---------------------------------------------------------------------------
APRS_HOST       = os.environ.get("APRS_HOST", "rotate.aprs2.net")
APRS_PORT       = int(os.environ.get("APRS_PORT", 14580))
APRS_CALLSIGN   = os.environ.get("APRS_CALLSIGN", "")
APRS_PASSWORD   = os.environ.get("APRS_PASSWORD", "-1")
MQTT_HOST       = os.environ.get("MQTT_HOST", "core-mosquitto")
MQTT_PORT       = int(os.environ.get("MQTT_PORT", 1883))
MQTT_USERNAME   = os.environ.get("MQTT_USERNAME", "")
MQTT_PASSWORD   = os.environ.get("MQTT_PASSWORD", "")
MQTT_PREFIX     = os.environ.get("MQTT_TOPIC_PREFIX", "aprs")
POLL_INTERVAL   = int(os.environ.get("POLL_INTERVAL", 30))

# Parse comma-separated callsign list, normalise to uppercase
_raw_callsigns  = os.environ.get("APRS_CALLSIGNS", "")
CALLSIGNS       = [c.strip().upper() for c in _raw_callsigns.split(",") if c.strip()]

# ---------------------------------------------------------------------------
# APRS symbol table (partial — most common symbols)
# ---------------------------------------------------------------------------
SYMBOL_TABLE = {
    "/$": "Car",
    "/!": "Police/Sheriff",
    "/#": "Digipeater",
    "/$": "Car",
    "/%": "Power",
    "/&": "Gateway",
    "/'": "Crash site",
    "/(": "Cloudy",
    "/)": "Fire dept",
    "/*": "Snow",
    "/+": "Church",
    "/,": "Boy Scouts",
    "/-": "House (QTH)",
    "/.": "X",
    "//": "Dot",
    "/0": "Circle (0)",
    "/A": "Ambulance",
    "/B": "Bike",
    "/C": "Incident cmd post",
    "/D": "Fire dept",
    "/E": "Horse",
    "/F": "Fire truck",
    "/G": "Glider",
    "/H": "Hospital",
    "/I": "IOTA",
    "/J": "Jeep",
    "/K": "School",
    "/L": "PC user",
    "/M": "Mac",
    "/N": "Node (black)",
    "/O": "Balloon",
    "/P": "Police",
    "/Q": "TBD",
    "/R": "RV",
    "/S": "Space shuttle",
    "/T": "SSTV",
    "/U": "Bus",
    "/V": "ATV",
    "/W": "National WX",
    "/X": "Helicopter",
    "/Y": "Yacht/sailboat",
    "/Z": "WinAPRS",
    "/[": "Jogger",
    "/\\": "Triangle",
    "/]": "PBBS",
    "/^": "Large aircraft",
    "/_": "WX station",
    "/`": "Dish antenna",
    "/a": "Ambulance",
    "/b": "Bike",
    "/c": "Incident cmd",
    "/d": "Dual garage",
    "/e": "Horse",
    "/f": "Fire truck",
    "/g": "Glider",
    "/h": "Hospital",
    "/i": "Island",
    "/j": "Jeep",
    "/k": "Truck",
    "/l": "Area",
    "/m": "MicE",
    "/n": "Node",
    "/o": "EOC",
    "/p": "Rover dog",
    "/q": "Grid sq",
    "/r": "Antenna",
    "/s": "Ship",
    "/t": "Truck stop",
    "/u": "Truck (18-wheeler)",
    "/v": "Van",
    "/w": "Water station",
    "/x": "X/Unix",
    "/y": "Yagi antenna",
    "/z": "Shelter",
}

def symbol_description(table, symbol):
    key = (table or "/") + (symbol or "?")
    return SYMBOL_TABLE.get(key, f"APRS ({key})")

# ---------------------------------------------------------------------------
# Slug helper  (N3EG-9 → n3eg_9)
# ---------------------------------------------------------------------------
def callsign_slug(callsign: str) -> str:
    return callsign.upper().replace("-", "_").lower()

# ---------------------------------------------------------------------------
# MQTT helpers
# ---------------------------------------------------------------------------
def discovery_topic(callsign: str) -> str:
    slug = callsign_slug(callsign)
    return f"homeassistant/device_tracker/{slug}/config"

def state_topic(callsign: str) -> str:
    slug = callsign_slug(callsign)
    return f"{MQTT_PREFIX}/device_tracker/{slug}/state"

def attributes_topic(callsign: str) -> str:
    slug = callsign_slug(callsign)
    return f"{MQTT_PREFIX}/device_tracker/{slug}/attributes"

def availability_topic(callsign: str) -> str:
    slug = callsign_slug(callsign)
    return f"{MQTT_PREFIX}/device_tracker/{slug}/availability"

def publish_discovery(client: mqtt.Client, callsign: str):
    """Publish HA MQTT auto-discovery message for a callsign."""
    slug = callsign_slug(callsign)
    payload = {
        "name": callsign.upper(),
        "unique_id": f"aprs_{slug}",
        "state_topic": state_topic(callsign),
        "json_attributes_topic": attributes_topic(callsign),
        "availability_topic": availability_topic(callsign),
        "payload_available": "online",
        "payload_not_available": "offline",
        "payload_home": "home",
        "payload_not_home": "not_home",
        "source_type": "gps",
        "icon": "mdi:radio-tower",
        "device": {
            "identifiers": [f"aprs_{slug}"],
            "name": f"APRS {callsign.upper()}",
            "model": "APRS Station",
            "manufacturer": "Amateur Radio",
        },
    }
    topic = discovery_topic(callsign)
    client.publish(topic, json.dumps(payload), retain=True)
    log.info(f"Published discovery for {callsign} → {topic}")

def publish_position(client: mqtt.Client, callsign: str, parsed: dict):
    """Publish state + attributes for a received APRS packet."""
    lat  = parsed.get("latitude")
    lon  = parsed.get("longitude")
    alt  = parsed.get("altitude")          # metres
    spd  = parsed.get("speed")             # km/h
    crs  = parsed.get("course")            # degrees
    cmnt = parsed.get("comment", "")
    sym_tbl  = parsed.get("symbol_table", "/")
    sym_code = parsed.get("symbol", "?")
    via  = parsed.get("path", "")
    raw  = parsed.get("raw", "")
    ts   = datetime.now(timezone.utc).isoformat()

    if lat is None or lon is None:
        log.warning(f"{callsign}: packet has no position, skipping.")
        return

    # HA device_tracker state is "home" or "not_home"; we always say not_home
    # (HA zone detection handles home determination from lat/lon)
    client.publish(state_topic(callsign), "not_home", retain=True)
    client.publish(availability_topic(callsign), "online", retain=True)

    attrs = {
        "latitude":    round(lat, 6),
        "longitude":   round(lon, 6),
        "gps_accuracy": 10,
    }
    if alt is not None:
        attrs["altitude"]      = round(alt, 1)
        attrs["altitude_ft"]   = round(alt * 3.28084, 1)
    if spd is not None:
        attrs["speed_kmh"]     = round(spd, 1)
        attrs["speed_mph"]     = round(spd * 0.621371, 1)
        attrs["speed_knots"]   = round(spd * 0.539957, 1)
    if crs is not None:
        attrs["course"]        = crs
    if cmnt:
        attrs["comment"]       = cmnt.strip()

    attrs["symbol"]            = symbol_description(sym_tbl, sym_code)
    attrs["symbol_raw"]        = f"{sym_tbl}{sym_code}"
    attrs["via"]               = via
    attrs["last_seen"]         = ts
    attrs["source"]            = "APRS-IS"

    client.publish(attributes_topic(callsign), json.dumps(attrs), retain=True)
    log.info(
        f"{callsign}: lat={attrs['latitude']} lon={attrs['longitude']}"
        + (f" alt={attrs.get('altitude_ft')}ft" if 'altitude_ft' in attrs else "")
        + (f" spd={attrs.get('speed_mph')}mph" if 'speed_mph' in attrs else "")
        + (f" crs={attrs.get('course')}°" if 'course' in attrs else "")
    )

# ---------------------------------------------------------------------------
# MQTT client setup
# ---------------------------------------------------------------------------
def build_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(client_id="aprs_mqtt_tracker", clean_session=True)
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD or None)

    def on_connect(c, userdata, flags, rc):
        if rc == 0:
            log.info(f"MQTT connected to {MQTT_HOST}:{MQTT_PORT}")
            # (Re)publish discovery for all callsigns after connect/reconnect
            for cs in CALLSIGNS:
                publish_discovery(c, cs)
                c.publish(availability_topic(cs), "offline", retain=True)
        else:
            log.error(f"MQTT connection failed, rc={rc}")

    def on_disconnect(c, userdata, rc):
        if rc != 0:
            log.warning("MQTT disconnected unexpectedly, will auto-reconnect.")

    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.will_set(f"{MQTT_PREFIX}/aprs_tracker/status", "offline", retain=True)
    return client

def mqtt_connect(client: mqtt.Client):
    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            client.loop_start()
            return
        except Exception as e:
            log.error(f"MQTT connect error: {e}  — retrying in 10s")
            time.sleep(10)

# ---------------------------------------------------------------------------
# APRS-IS connection
# ---------------------------------------------------------------------------
# Build the APRS-IS filter string from our callsign list
def build_aprs_filter() -> str:
    parts = [f"b/{cs}" for cs in CALLSIGNS]
    return " ".join(parts)

class APRSTracker:
    def __init__(self, mqtt_client: mqtt.Client):
        self.mqtt   = mqtt_client
        self.ais    = None
        self._stop  = threading.Event()

    def _callback(self, raw_packet):
        try:
            parsed = aprslib.parse(raw_packet)
        except Exception as e:
            log.debug(f"Parse error: {e}")
            return

        frm = parsed.get("from", "").upper()
        if frm in CALLSIGNS:
            publish_position(self.mqtt, frm, parsed)

    def _connect(self):
        filt = build_aprs_filter()
        log.info(f"Connecting to APRS-IS {APRS_HOST}:{APRS_PORT} filter='{filt}'")
        self.ais = aprslib.IS(
            APRS_CALLSIGN,
            passwd=APRS_PASSWORD,
            host=APRS_HOST,
            port=APRS_PORT,
        )
        self.ais.set_filter(filt)
        self.ais.connect()
        log.info("APRS-IS connected.")

    def run(self):
        while not self._stop.is_set():
            try:
                self._connect()
                self.ais.consumer(self._callback, raw=True, immortal=False)
            except StopIteration:
                log.info("APRS-IS stream ended.")
            except Exception as e:
                log.error(f"APRS-IS error: {e}")
            if not self._stop.is_set():
                log.info("Reconnecting to APRS-IS in 15s...")
                time.sleep(15)

    def stop(self):
        self._stop.set()
        if self.ais:
            try:
                self.ais.close()
            except Exception:
                pass

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    if not CALLSIGNS:
        log.error("No callsigns configured. Set at least one callsign in the add-on options.")
        sys.exit(1)

    if not APRS_CALLSIGN:
        log.error("No APRS login callsign configured (aprs_callsign).")
        sys.exit(1)

    log.info(f"Tracking: {', '.join(CALLSIGNS)}")

    # MQTT
    mqtt_client = build_mqtt_client()
    mqtt_connect(mqtt_client)
    time.sleep(2)  # let on_connect fire

    # APRS tracker (runs in main thread)
    tracker = APRSTracker(mqtt_client)

    def shutdown(signum, frame):
        log.info("Shutting down...")
        tracker.stop()
        for cs in CALLSIGNS:
            mqtt_client.publish(availability_topic(cs), "offline", retain=True)
        time.sleep(1)
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT,  shutdown)

    tracker.run()

if __name__ == "__main__":
    main()
