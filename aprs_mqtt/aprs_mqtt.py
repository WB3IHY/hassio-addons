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
import urllib.request
import urllib.parse
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
POLL_INTERVAL   = int(os.environ.get("POLL_INTERVAL", 30))  # minutes between aprs.fi fallback polls
APRSFI_API_KEY  = os.environ.get("APRSFI_API_KEY", "")
APRSFI_URL      = "https://api.aprs.fi/api/get"

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

def publish_position(client: mqtt.Client, callsign: str, parsed: dict,
                      source: str = "APRS-IS", seen_ts: str = None):
    """Publish state + attributes for a received or polled APRS position."""
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
    ts   = seen_ts or datetime.now(timezone.utc).isoformat()

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
    attrs["source"]            = source

    client.publish(attributes_topic(callsign), json.dumps(attrs), retain=True)
    log.info(
        f"{callsign} [{source}]: lat={attrs['latitude']} lon={attrs['longitude']}"
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
# Track when each callsign was last heard live on APRS-IS, so the aprs.fi
# fallback poller knows which callsigns still need a cached lookup.
# ---------------------------------------------------------------------------
_live_seen_lock = threading.Lock()
_live_seen = {}  # callsign -> time.monotonic() of last live packet

def mark_live_seen(callsign: str):
    with _live_seen_lock:
        _live_seen[callsign] = time.monotonic()

def seconds_since_live(callsign: str):
    with _live_seen_lock:
        t = _live_seen.get(callsign)
    return None if t is None else time.monotonic() - t

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
            mark_live_seen(frm)

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
# aprs.fi fallback polling
# ---------------------------------------------------------------------------
def aprsfi_lookup(callsigns: list) -> dict:
    """Query aprs.fi for the last-known position of each callsign.

    Returns {callsign: parsed_dict} using the same keys as aprslib's parsed
    packets (plus "lasttime"), so results can be passed to publish_position().
    """
    if not APRSFI_API_KEY or not callsigns:
        return {}

    query = urllib.parse.urlencode({
        "name":   ",".join(callsigns),
        "what":   "loc",
        "apikey": APRSFI_API_KEY,
        "format": "json",
    })
    try:
        with urllib.request.urlopen(f"{APRSFI_URL}?{query}", timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        log.warning(f"aprs.fi lookup failed: {e}")
        return {}

    if data.get("result") != "ok":
        log.warning(f"aprs.fi lookup error: {data.get('description', data)}")
        return {}

    results = {}
    for entry in data.get("entries", []):
        name = entry.get("name", "").upper()
        try:
            lat = float(entry["lat"])
            lon = float(entry["lng"])
        except (KeyError, TypeError, ValueError):
            continue
        symbol = entry.get("symbol") or "/?"
        results[name] = {
            "latitude":     lat,
            "longitude":    lon,
            "altitude":     float(entry["altitude"]) if entry.get("altitude") is not None else None,
            "speed":        float(entry["speed"]) if entry.get("speed") is not None else None,
            "course":       float(entry["course"]) if entry.get("course") is not None else None,
            "comment":      entry.get("comment", ""),
            "symbol_table": symbol[0] if symbol else "/",
            "symbol":       symbol[1] if len(symbol) > 1 else "?",
            "path":         entry.get("path", ""),
            "lasttime":     entry.get("lasttime"),
        }
    return results

class AprsFiPoller:
    """Polls aprs.fi for last-known positions: once immediately at startup
    for every configured callsign, then periodically as a fallback for
    whichever callsigns haven't been heard live on APRS-IS recently."""

    def __init__(self, mqtt_client: mqtt.Client, interval_minutes: int):
        self.mqtt     = mqtt_client
        self.interval = max(1, interval_minutes) * 60
        self._stop    = threading.Event()

    def _poll(self, callsigns):
        results = aprsfi_lookup(callsigns)
        for cs in callsigns:
            entry = results.get(cs)
            if not entry:
                continue
            lasttime = entry.pop("lasttime", None)
            seen_ts = (
                datetime.fromtimestamp(int(lasttime), tz=timezone.utc).isoformat()
                if lasttime else None
            )
            publish_position(self.mqtt, cs, entry, source="aprs.fi", seen_ts=seen_ts)

    def run(self):
        log.info("Polling aprs.fi for initial positions...")
        self._poll(CALLSIGNS)

        while not self._stop.wait(self.interval):
            stale = [cs for cs in CALLSIGNS
                     if seconds_since_live(cs) is None or seconds_since_live(cs) > self.interval]
            if stale:
                log.info(f"Re-polling aprs.fi for stale callsigns: {', '.join(stale)}")
                self._poll(stale)

    def stop(self):
        self._stop.set()

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

    # aprs.fi fallback poller (optional, runs in background thread)
    aprsfi_poller = None
    if APRSFI_API_KEY:
        aprsfi_poller = AprsFiPoller(mqtt_client, POLL_INTERVAL)
        threading.Thread(target=aprsfi_poller.run, daemon=True).start()
    else:
        log.info("aprsfi_api_key not set — skipping aprs.fi startup/fallback polling.")

    def shutdown(signum, frame):
        log.info("Shutting down...")
        tracker.stop()
        if aprsfi_poller:
            aprsfi_poller.stop()
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
