#!/usr/bin/env python3
"""
Paradigma remoteportal.de -> MQTT Bridge fuer Home Assistant.

Liest die Anlagenwerte (SystaComfort / SystaSolar) von
https://paradigma.remoteportal.de per Playwright (echter Browser,
weil die Werte erst per JS/AJAX nachgeladen werden) und published
sie per MQTT mit Home-Assistant-Discovery, sodass HA die Sensoren
automatisch anlegt.

Zugangsdaten kommen NIE als Klartext ins Skript, sondern ausschliesslich
ueber Umgebungsvariablen (siehe .env.example).

Aufbau der Zielseite (Stand: manuelle Analyse, siehe Kommentare unten):
  - table.elementList  -> ein Panel (z.B. "Werte SystaComfort")
  - erste <tr> im Panel: <th> = Panel-Titel
  - weitere <tr>:
      * Zwischenueberschrift:  ein <td class="title"> ohne Wert
      * Datenzeile: <td class="title right"> Label
                    <td class="number"[ bit-on|bit-off]"> Wert
                    <td class="center"> Einheit (kann leer sein)
"""

import asyncio
import json
import logging
import os
import re
import sys
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path

from playwright.async_api import async_playwright, Page, TimeoutError as PWTimeout

try:
    import paho.mqtt.client as mqtt
except ImportError:  # erlaubt --dry-run ohne installiertes paho-mqtt
    mqtt = None

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("paradigma-scraper")

# ----------------------------------------------------------------------
# Konfiguration ueber Umgebungsvariablen
# ----------------------------------------------------------------------

PARADIGMA_USER = os.environ["PARADIGMA_USER"]
PARADIGMA_PASS = os.environ["PARADIGMA_PASS"]
PORTAL_URL = os.environ.get("PARADIGMA_URL", "https://paradigma.remoteportal.de/")

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER")
MQTT_PASS = os.environ.get("MQTT_PASS")
MQTT_BASE_TOPIC = os.environ.get("MQTT_BASE_TOPIC", "paradigma")
MQTT_DISCOVERY_PREFIX = os.environ.get("MQTT_DISCOVERY_PREFIX", "homeassistant")

POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "300"))  # 5 Min.
# Beobachtetes Auto-Refresh-Intervall der Seite lag bei wenigen Sekunden;
# nach dem Laden warten wir grosszuegig, damit der erste AJAX-Zyklus sicher
# durch ist, bevor wir aus dem DOM lesen.
POST_LOAD_WAIT_SECONDS = float(os.environ.get("POST_LOAD_WAIT_SECONDS", "8"))

STATE_DIR = Path(os.environ.get("STATE_DIR", "/data"))
STORAGE_STATE_FILE = STATE_DIR / "storage_state.json"

DEVICE_NAME = os.environ.get("DEVICE_NAME", "Paradigma Pelletsheizung")
DEVICE_ID = os.environ.get("DEVICE_ID", "paradigma_heizung")

# ----------------------------------------------------------------------
# Filter: welche Werte ueberhaupt published werden
# ----------------------------------------------------------------------
# Einzelne Labels, die unabhaengig vom Panel immer raus fliegen (nicht
# angeschlossene/nicht genutzte Fuehler, unplausible Werte).
EXCLUDE_LABELS = {
    "Pelletslager Füllstand",  # kein Fuellstandssensor vorhanden
    "Reg116",
    "Reg117",
    "Zirkulation (TZR)",  # -30,0 °C - nicht angeschlossener Fuehler
}

# Ganze Panels, die standardmaessig raus fliegen (statische Konfiguration,
# manuelle Override-Schalter - aendert sich praktisch nie).
EXCLUDE_PANELS = {
    "Einsteller SystaComfort",
    "Einsteller SystaSolar Aqua II",
    "Schalter",
}

# Einzelne (Panel, Label)-Kombinationen, die TROTZ ausgeschlossenem Panel
# behalten werden sollen.
KEEP_OVERRIDES = {
    ("Einsteller SystaSolar Aqua II", "Warmwasser Sollwert"),
}


def should_keep(panel: str, label: str) -> bool:
    if (panel, label) in KEEP_OVERRIDES:
        return True
    if label in EXCLUDE_LABELS:
        return False
    if panel in EXCLUDE_PANELS:
        return False
    return True


# ----------------------------------------------------------------------
# Datenmodell
# ----------------------------------------------------------------------

@dataclass
class DataPoint:
    panel: str
    label: str
    value: str
    unit: str = ""
    is_bit: bool = False  # Schalter/Status (on/off), statt Messwert
    group: str = ""  # Zwischenueberschrift innerhalb des Panels, falls vorhanden
    dedupe_suffix: str = ""  # wird ggf. nachtraeglich gesetzt bei Kollisionen

    @property
    def _base_key(self) -> str:
        parts = [self.panel, self.group, self.label]
        return "_".join(p for p in parts if p)

    @property
    def unique_id(self) -> str:
        slug = slugify(self._base_key + self.dedupe_suffix)
        return f"{DEVICE_ID}_{slug}"

    @property
    def topic(self) -> str:
        slug = slugify(self._base_key + self.dedupe_suffix)
        return f"{MQTT_BASE_TOPIC}/{slug}"

    def _clean_group(self) -> str:
        """Entfernt aus der Gruppe Woerter, die schon im Panel-Namen stehen
        (z.B. Panel "Ausgänge" + Gruppe "Ausgänge SystaSolar Aqua II" ->
        nur noch "SystaSolar Aqua II"), und laesst die Gruppe komplett weg,
        falls danach nichts Sinnvolles mehr uebrig ist oder sie eh dem
        Label entspricht (Panel "Werte SystaComfort" + Gruppe "Taster" +
        Label "Taster" -> keine Gruppe anzeigen)."""
        if not self.group:
            return ""
        panel_words = set(self.panel.split())
        remaining = [w for w in self.group.split() if w not in panel_words]
        cleaned = " ".join(remaining)
        if not cleaned or cleaned == self.label:
            return ""
        return cleaned

    @property
    def display_name(self) -> str:
        parts = [self.panel, self._clean_group(), self.label]
        name = " ".join(p for p in parts if p)
        if self.dedupe_suffix:
            name = f"{name} ({self.dedupe_suffix.lstrip('_')})"
        return name


def slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    text = re.sub(r"[^a-zA-Z0-9]+", "_", text).strip("_").lower()
    return text


# ----------------------------------------------------------------------
# Scraping
# ----------------------------------------------------------------------

async def is_login_page(page: Page) -> bool:
    return await page.locator('input[type="password"]').count() > 0


async def login(page: Page) -> None:
    log.info("Nicht eingeloggt - fuehre Login durch")
    await page.goto(PORTAL_URL, wait_until="domcontentloaded")

    if not await is_login_page(page):
        log.info("Bereits eingeloggt (Session war noch gueltig)")
        return

    password_field = page.locator('input[type="password"]').first
    # Das Feld VOR dem Passwortfeld im selben Formular ist praktisch immer
    # das Benutzername-Feld - so bleibt der Login robust, auch wenn sich
    # die genauen Feldnamen im Portal mal aendern.
    form = password_field.locator("xpath=ancestor::form[1]")
    username_field = form.locator(
        'input[type="text"], input[type="email"], input:not([type])'
    ).first

    await username_field.fill(PARADIGMA_USER)
    await password_field.fill(PARADIGMA_PASS)

    submit = form.locator('button[type="submit"], input[type="submit"]').first
    if await submit.count():
        await submit.click()
    else:
        await password_field.press("Enter")

    await page.wait_for_load_state("networkidle", timeout=15000)

    if await is_login_page(page):
        raise RuntimeError(
            "Login fehlgeschlagen - Zugangsdaten pruefen oder Portal-Struktur "
            "hat sich geaendert (Login-Formular sieht anders aus)."
        )
    log.info("Login erfolgreich")


async def goto_controller_page(page: Page, context) -> None:
    url = PORTAL_URL.rstrip("/") + "/index.php?pto[area]=-controller"
    await page.goto(url, wait_until="domcontentloaded")
    if await is_login_page(page):
        await login(page)
        await page.goto(url, wait_until="domcontentloaded")

    # Auf den ersten AJAX-Refresh-Zyklus warten, sonst lesen wir nur "---".
    await asyncio.sleep(POST_LOAD_WAIT_SECONDS)

    # Zusaetzliche Absicherung: die Session kann serverseitig ungueltig
    # werden, OHNE dass wieder ein klassisches Login-Formular erscheint -
    # z.B. bei einer Single-Session-Beschraenkung, wenn parallel im Browser
    # eingeloggt wird. is_login_page() erkennt das dann nicht. Als Signal
    # nehmen wir stattdessen: keine einzige Datentabelle vorhanden, obwohl
    # wir eigentlich "eingeloggt" sind. In dem Fall Cookies hart loeschen
    # und komplett frisch einloggen.
    table_count = await page.locator("table.elementList").count()
    if table_count == 0:
        log.warning(
            "Keine Datentabellen gefunden, obwohl keine Login-Seite angezeigt "
            "wurde (vermutlich serverseitig abgelaufene/ungueltige Session, "
            "z.B. durch parallelen Browser-Login) - erzwinge frischen Login"
        )
        await context.clear_cookies()
        await login(page)
        await page.goto(url, wait_until="domcontentloaded")
        await asyncio.sleep(POST_LOAD_WAIT_SECONDS)


async def scrape_values(page: Page) -> list[DataPoint]:
    panels = await page.evaluate(
        """
        () => {
            const tables = document.querySelectorAll('table.elementList');
            return Array.from(tables).map(t => {
                const rows = Array.from(t.querySelectorAll('tr'));
                const title = rows[0]?.querySelector('th')?.textContent.trim() || '';
                const items = [];
                let group = '';
                for (const r of rows.slice(1)) {
                    const cells = Array.from(r.children);
                    if (cells.length < 2) {
                        // Zwischenueberschrift (z.B. "Schalter HK 1:") - merken statt
                        // ueberspringen, damit gleichnamige Labels in verschiedenen
                        // Abschnitten (z.B. "Status Zirkulation") unterscheidbar bleiben.
                        const headerText = cells[0] ? cells[0].textContent.trim() : '';
                        if (headerText) group = headerText.replace(/:$/, '');
                        continue;
                    }
                    const label = cells[0].textContent.trim();
                    const valueCell = cells[1];
                    const value = valueCell.textContent.trim();
                    const unit = cells[2] ? cells[2].textContent.trim() : '';
                    const isBit = valueCell.className.includes('bit-');
                    if (!label || value === '') continue;
                    items.push({label, value, unit, isBit, group});
                }
                return {title, items};
            });
        }
        """
    )

    points: list[DataPoint] = []
    seen_keys: dict[str, int] = {}
    skipped = 0
    for panel in panels:
        for item in panel["items"]:
            if not should_keep(panel["title"], item["label"]):
                skipped += 1
                continue
            dp = DataPoint(
                panel=panel["title"],
                label=item["label"],
                value=item["value"],
                unit=item["unit"],
                is_bit=item["isBit"],
                group=item["group"],
            )
            # Letzte Absicherung: falls Panel+Gruppe+Label trotzdem doppelt
            # vorkommt, Suffix anhaengen statt Werte stillschweigend zu verlieren.
            base = dp._base_key
            count = seen_keys.get(base, 0)
            if count:
                dp.dedupe_suffix = f"_{count + 1}"
                log.warning(
                    "Doppeltes Label '%s' erkannt, verwende Suffix '%s'",
                    base, dp.dedupe_suffix,
                )
            seen_keys[base] = count + 1
            points.append(dp)
    log.debug("%d Werte durch Filter ausgeschlossen", skipped)
    return points


# ----------------------------------------------------------------------
# MQTT
# ----------------------------------------------------------------------

def build_mqtt_client() -> "mqtt.Client":
    if mqtt is None:
        raise RuntimeError("paho-mqtt ist nicht installiert (pip install paho-mqtt)")
    client = mqtt.Client(client_id=DEVICE_ID, protocol=mqtt.MQTTv311)
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    return client


_discovery_sent: set[str] = set()


def publish_discovery(client: "mqtt.Client", dp: DataPoint) -> None:
    if dp.unique_id in _discovery_sent:
        return
    _discovery_sent.add(dp.unique_id)

    component = "binary_sensor" if dp.is_bit else "sensor"
    config_topic = f"{MQTT_DISCOVERY_PREFIX}/{component}/{dp.unique_id}/config"

    payload = {
        "name": dp.display_name,
        "unique_id": dp.unique_id,
        "state_topic": dp.topic,
        "device": {
            "identifiers": [DEVICE_ID],
            "name": DEVICE_NAME,
            "manufacturer": "Paradigma",
            "model": "SystaComfort (remoteportal.de)",
        },
    }
    if dp.is_bit:
        payload["payload_on"] = "on"
        payload["payload_off"] = "off"
    else:
        if dp.unit:
            payload["unit_of_measurement"] = dp.unit
        if dp.unit == "°C":
            payload["device_class"] = "temperature"
        elif dp.unit == "%":
            payload["device_class"] = "power_factor"
        elif dp.unit == "kWh":
            payload["device_class"] = "energy"
            payload["state_class"] = "total_increasing"

    client.publish(config_topic, json.dumps(payload), retain=True)


def publish_value(client: "mqtt.Client", dp: DataPoint) -> None:
    value = dp.value
    if dp.is_bit:
        value = "on" if value.strip().lower() == "on" else "off"
    else:
        # deutsches Dezimalkomma -> Punkt, damit HA es als Zahl erkennt
        value = value.replace(".", "").replace(",", ".")
        if value in ("", "---", "-", "--"):
            return  # kein Messwert vorhanden - lieber nichts senden als 0
    client.publish(dp.topic, value, retain=True)


# ----------------------------------------------------------------------
# Hauptschleife
# ----------------------------------------------------------------------

async def run_once(page: Page, client, context) -> int:
    await goto_controller_page(page, context)
    points = await scrape_values(page)
    log.info("%d Datenpunkte gelesen", len(points))

    for dp in points:
        if client is not None:
            publish_discovery(client, dp)
            publish_value(client, dp)
        else:
            print(f"{dp.display_name}: {dp.value} {dp.unit}".strip())

    return len(points)


async def main() -> None:
    dry_run = "--dry-run" in sys.argv
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    client = None
    if not dry_run:
        client = build_mqtt_client()
        client.loop_start()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        storage_state = str(STORAGE_STATE_FILE) if STORAGE_STATE_FILE.exists() else None
        context = await browser.new_context(storage_state=storage_state)
        page = await context.new_page()

        try:
            await login(page)
        except PWTimeout:
            log.exception("Timeout beim Login")
            raise

        await context.storage_state(path=str(STORAGE_STATE_FILE))

        while True:
            try:
                await run_once(page, client, context)
                await context.storage_state(path=str(STORAGE_STATE_FILE))
            except Exception:  # noqa: BLE001 - Skript soll weiterlaufen
                log.exception("Fehler beim Scrape-Durchlauf, versuche es erneut")

            if dry_run:
                break
            await asyncio.sleep(POLL_INTERVAL)

        await browser.close()

    if client is not None:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())