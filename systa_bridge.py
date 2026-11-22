#!/usr/bin/env python3
"""
SystaComfort II -> MQTT Bridge (lokal, ohne Modbus, ohne Web-Portal).

Nutzt die lokale Service-Schnittstelle der SystaComfort II auf UDP-Port 7260
(dieselbe, die die Paradigma-Wartungssoftware "SystaService" fuer das
Monitoring verwendet). Reverse engineered, getestet mit FW V1.34.1.

Protokoll
---------
Anfrage (an Regler:7260):  b"<UDP-Passwort>\\n" + Frame
Antwort (vom Regler):      4 Byte Zeitstempel + Frame
Frame:  [Typ] [Len] [Nutzdaten ... Len Bytes] [Pruefsumme]
  Typ 0x0a = Echo der Anfrage, 0xfc = Monitoring-Daten, 0xfd = Antwort,
  0xfa = Bus-Telegramm. Anfragen beginnen direkt mit [Len].
  Pruefsumme: Summe aller Bytes (Anfrage: ab Len, mit 0x0a davor) = 0 mod 256.

Monitoring starten (SystaService-Sequenz beim Klick auf "Start"):
  01 16  -> Bus-Monitor an
  01 17  -> Monitoring SystaComfort an
  03 1c 0c 18
  01 14  -> Monitoring Solarregler an
Danach sendet der Regler alle ~10 s:
  fc 29 0c 01 ...  Messwerte SystaComfort (int16 big endian, /10 °C)
  fc 29 0c 02 ...  Sollwerte/Zaehler SystaComfort
  fc 3e 24 01 ...  Messwerte SystaSolar Aqua II (int16 little endian)
Ohne erneute Anfrage endet der Strom nach ~60 s -> alle KEEPALIVE s wiederholen.

Nur lesend: Es werden ausschliesslich die Monitoring-Befehle gesendet.
"""

import json
import logging
import os
import socket
import struct
import time

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("systa-bridge")

SYSTA_HOST = os.environ.get("SYSTA_HOST", "")  # IP der SystaComfort II (Pflicht)
SYSTA_PORT = int(os.environ.get("SYSTA_PORT", "7260"))
SYSTA_PASS = os.environ.get("SYSTA_PASS", "1234")
KEEPALIVE = float(os.environ.get("KEEPALIVE_SECONDS", "30"))
STALE_AFTER = float(os.environ.get("STALE_AFTER_SECONDS", "120"))

MQTT_HOST = os.environ.get("MQTT_HOST", "")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER")
MQTT_PASS = os.environ.get("MQTT_PASS")
BASE_TOPIC = os.environ.get("MQTT_BASE_TOPIC", "systa")
DISCOVERY_PREFIX = os.environ.get("MQTT_DISCOVERY_PREFIX", "homeassistant")
DEVICE_ID = os.environ.get("DEVICE_ID", "systacomfort2")
DEVICE_NAME = os.environ.get("DEVICE_NAME", "SystaComfort II (lokal)")
RAW_DEBUG = os.environ.get("RAW_DEBUG", "0") == "1"
# Korrelations-Log (zum Entschluesseln weiterer Felder): schreibt Rohframes
# und - falls CORRELATE_TOPIC gesetzt - Werte des alten Portal-Scrapers bei
# jeder Aenderung mit Zeitstempel in eine Datei.
RAW_LOG_FILE = os.environ.get("RAW_LOG_FILE", "")
CORRELATE_TOPIC = os.environ.get("CORRELATE_TOPIC", "")

START_SEQUENCE = ["16", "17", "1c0c18", "14"]  # Laengenbyte ergaenzt frame()

# ----------------------------------------------------------------------
# Feldzuordnung. Offset = Byte-Position in den Nutzdaten NACH
# [Typ][Len][Geraet][Block]. Verifiziert gegen SystaService-Monitoring
# und Portal-Scraper.
# (offset, key, name, unit, device_class, divisor, signed, bytes, endian)
# ----------------------------------------------------------------------
SENSOR_NA = -300  # 0xFED4 = -30,0 °C = Fuehler nicht angeschlossen

COMFORT_1 = [  # fc 29 0c 01, big endian
    (4, "aussentemperatur", "Außentemperatur TA", "°C", "temperature", 10),
    (6, "warmwasser", "Warmwasser TWO", "°C", "temperature", 10),
    (8, "kessel_vorlauf", "Kessel Vorlauf FA TV", "°C", "temperature", 10),
    (10, "kessel_ruecklauf", "Kessel Rücklauf FA TR", "°C", "temperature", 10),
    (12, "hk1_raum", "HK1 Raum TI", "°C", "temperature", 10),
    (14, "hk2_raum", "HK2 Raum TI2", "°C", "temperature", 10),
    (16, "hk1_vorlauf", "HK1 Vorlauf TV", "°C", "temperature", 10),
    (18, "hk2_vorlauf", "HK2 Vorlauf TV2", "°C", "temperature", 10),
    (20, "hk1_ruecklauf", "HK1 Rücklauf TR", "°C", "temperature", 10),
    (22, "hk2_ruecklauf", "HK2 Rücklauf TR2", "°C", "temperature", 10),
    (24, "puffer_oben", "Puffer oben TPO", "°C", "temperature", 10),
    (26, "puffer_unten", "Puffer unten TPU", "°C", "temperature", 10),
    (28, "zirkulation", "Zirkulation TZR", "°C", "temperature", 10),
]
COMFORT_2 = [  # fc 29 0c 02, big endian
    (0, "hk1_raum_soll", "HK1 Raum Soll", "°C", "temperature", 10),
    (2, "hk2_raum_soll", "HK2 Raum Soll", "°C", "temperature", 10),
    (4, "hk1_vorlauf_soll", "HK1 Vorlauf Soll", "°C", "temperature", 10),
    (6, "hk2_vorlauf_soll", "HK2 Vorlauf Soll", "°C", "temperature", 10),
    (8, "warmwasser_soll", "Warmwasser Soll", "°C", "temperature", 10),
    (10, "puffer_soll", "Puffer Soll", "°C", "temperature", 10),
    (28, "kesselpumpe", "Kesselpumpe PK", "%", None, 1),
    (30, "hk1_pumpe", "HK1 Pumpe PHK", "%", None, 1),
    (32, "hk2_pumpe", "HK2 Pumpe PHK2", "%", None, 1),
]
COMFORT_2_U32 = [
    (14, "kessel_betriebsstunden", "Kessel Betriebsstunden", "h", "duration"),
    (18, "kessel_starts", "Kessel Starts", None, None),
]
SOLAR_1 = [  # fc 3e 24 01, little endian
    (0, "solar_kollektor", "Solar Kollektor TSA", "°C", "temperature", 10),
    (2, "solar_speicher_oben", "Solar Speicher oben TW", "°C", "temperature", 10),
    (4, "solar_vorlauf", "Solar Vorlauf TSV", "°C", "temperature", 10),
    (6, "solar_aussen", "Solar Außenfühler TAM", "°C", "temperature", 10),
    (10, "solar_ruecklauf", "Solar Rücklauf TSE", "°C", "temperature", 10),
]


class RawLog:
    """Schreibt Zeilen '<epoch> RAW <hex>' / '<epoch> MQTT <topic> <wert>',
    jeweils nur bei Aenderung gegenueber dem letzten Stand."""

    def __init__(self, path):
        self.f = open(path, "a", encoding="utf-8") if path else None
        self.last = {}

    def write(self, kind, key, value):
        if not self.f or self.last.get((kind, key)) == value:
            return
        self.last[(kind, key)] = value
        self.f.write(f"{time.time():.0f} {kind} {key} {value}\n")
        self.f.flush()


def frame(payload_hex: str) -> bytes:
    body = bytes.fromhex(payload_hex)
    body = bytes([len(body)]) + body
    cs = (-(0x0A + sum(body))) & 0xFF
    return SYSTA_PASS.encode() + b"\n" + body + bytes([cs])


class Mqtt:
    def __init__(self):
        self.c = None
        self.sent = set()
        if not MQTT_HOST or mqtt is None:
            log.info("MQTT deaktiviert -> Werte nur im Log")
            return
        self.c = mqtt.Client(client_id=DEVICE_ID)
        if MQTT_USER:
            self.c.username_pw_set(MQTT_USER, MQTT_PASS)
        self.c.will_set(f"{BASE_TOPIC}/status", "offline", retain=True)
        self.c.on_connect = self._on_connect
        self.c.on_message = self._on_message
        self.c.connect(MQTT_HOST, MQTT_PORT, 60)
        self.c.loop_start()
        self.c.publish(f"{BASE_TOPIC}/status", "online", retain=True)

    rawlog = None

    def _on_connect(self, client, userdata, flags, rc):
        client.publish(f"{BASE_TOPIC}/status", "online", retain=True)
        if CORRELATE_TOPIC:
            client.subscribe(CORRELATE_TOPIC)

    def _on_message(self, client, userdata, msg):
        if self.rawlog:
            self.rawlog.write("MQTT", msg.topic, msg.payload.decode(errors="replace").replace(" ", "_"))

    def pub(self, key, name, value, unit=None, dclass=None):
        if not self.c:
            log.info("%-28s %s %s", name, value, unit or "")
            return
        uid = f"{DEVICE_ID}_{key}"
        topic = f"{BASE_TOPIC}/{key}"
        if uid not in self.sent:
            cfg = {"name": name, "unique_id": uid, "state_topic": topic,
                   "availability_topic": f"{BASE_TOPIC}/status",
                   "device": {"identifiers": [DEVICE_ID], "name": DEVICE_NAME,
                              "manufacturer": "Paradigma", "model": "SystaComfort II"}}
            if unit:
                cfg["unit_of_measurement"] = unit
            if dclass:
                cfg["device_class"] = dclass
            if dclass != "timestamp":
                cfg["state_class"] = ("total_increasing"
                                      if key in ("kessel_starts", "kessel_betriebsstunden")
                                      else "measurement")
            self.c.publish(f"{DISCOVERY_PREFIX}/sensor/{uid}/config", json.dumps(cfg), retain=True)
            self.sent.add(uid)
        self.c.publish(topic, "" if value is None else value, retain=True)


def i16(data, off, big):
    return struct.unpack_from(">h" if big else "<h", data, off)[0]


def publish_fields(mq, data, fields, big):
    for off, key, name, unit, dclass, div in fields:
        if off + 2 > len(data):
            continue
        raw = i16(data, off, big)
        if dclass == "temperature" and raw == SENSOR_NA:
            continue  # Fuehler nicht angeschlossen -> nicht anlegen
        mq.pub(key, name, round(raw / div, 1) if div != 1 else raw, unit, dclass)


def handle(mq, pkt: bytes, rawlog=None):
    if len(pkt) < 8:
        return
    f = pkt[4:]
    typ, ln = f[0], f[1]
    body = f[2:2 + ln]
    if sum(f[:2 + ln + 1]) & 0xFF and sum(f[1:2 + ln + 1]) & 0xFF:
        log.debug("Pruefsumme falsch: %s", f.hex())
        return
    if RAW_DEBUG and typ in (0xFC, 0xFD):
        log.info("RAW %s", f.hex(" "))
    if typ != 0xFC or len(body) < 2:
        return
    if rawlog:
        rawlog.write("RAW", f"{body[0]:02x}{body[1]:02x}", body[2:].hex())
    dev, blk, data = body[0], body[1], body[2:]
    if dev == 0x0C and blk == 0x01:
        publish_fields(mq, data, COMFORT_1, True)
        mq.pub("letzte_aktualisierung", "Letzte Aktualisierung",
               time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()), None, "timestamp")
    elif dev == 0x0C and blk == 0x02:
        publish_fields(mq, data, COMFORT_2, True)
        for off, key, name, unit, dclass in COMFORT_2_U32:
            mq.pub(key, name, struct.unpack_from(">I", data, off)[0], unit, dclass)
    elif dev == 0x24 and blk == 0x01:
        publish_fields(mq, data, SOLAR_1, False)


def main():
    rawlog = RawLog(RAW_LOG_FILE)
    Mqtt.rawlog = rawlog
    if not SYSTA_HOST:
        raise SystemExit("SYSTA_HOST ist nicht gesetzt (IP der SystaComfort II, siehe .env.example)")
    mq = Mqtt()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", 0))
    sock.settimeout(1.0)
    dest = (SYSTA_HOST, SYSTA_PORT)
    log.info("Bridge startet: %s:%d", *dest)
    last_start = 0.0
    last_data = time.time()
    online = True
    while True:
        now = time.time()
        if now - last_start >= KEEPALIVE:
            for cmd in START_SEQUENCE:
                sock.sendto(frame(cmd), dest)
                time.sleep(0.3)
            last_start = now
        try:
            pkt, addr = sock.recvfrom(4096)
        except socket.timeout:
            if online and time.time() - last_data > STALE_AFTER:
                log.warning("Seit %ds keine Daten von der Systa", STALE_AFTER)
                if mq.c:
                    mq.c.publish(f"{BASE_TOPIC}/status", "offline", retain=True)
                online = False
            continue
        if addr[0] != SYSTA_HOST:
            continue
        if pkt[4:5] == b"\xfc":
            last_data = time.time()
            if not online:
                log.info("Daten kommen wieder")
                if mq.c:
                    mq.c.publish(f"{BASE_TOPIC}/status", "online", retain=True)
                online = True
        try:
            handle(mq, pkt, rawlog)
        except Exception:  # noqa: BLE001
            log.exception("Fehler beim Verarbeiten: %s", pkt.hex())


if __name__ == "__main__":
    main()
