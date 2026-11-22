# Paradigma remoteportal.de → MQTT → Home Assistant

> Älterer Ansatz, durch die lokale Bridge im Hauptverzeichnis ersetzt.
> Benötigt ein (kostenpflichtiges) Konto beim Paradigma-Fernwartungsportal.

Liest die Anlagenwerte einer Paradigma Pellets-Heizung (SystaComfort ohne
Modbus-Schnittstelle) vom Fernwartungsportal `paradigma.remoteportal.de`
und stellt sie per MQTT (mit Home-Assistant-Discovery) zur Verfügung.

## Funktionsweise

- Läuft ein headless Chromium (Playwright), weil die Werte auf der Portalseite
  erst per JavaScript/AJAX nachgeladen werden – ein reiner HTTP-Scraper würde
  nur leere Platzhalter (`---`) sehen.
- Loggt sich einmalig ein, hebt die Session in `/data/storage_state.json` auf
  und nutzt sie bei weiteren Durchläufen wieder (kein Login bei jedem Zyklus).
- Liest alle 7 Tabellen der Seite (Werte SystaComfort, Einsteller
  SystaComfort, Werte SystaSolar Aqua II, Einsteller SystaSolar Aqua II,
  Statusanzeigen, Ausgänge, Schalter) und published jeden Wert einzeln per
  MQTT.
- Legt beim ersten Durchlauf automatisch die passenden Home-Assistant-
  Entities per MQTT Discovery an (`homeassistant/sensor/...` bzw.
  `homeassistant/binary_sensor/...` für Schalter/Status).

## Setup

1. `.env.example` nach `.env` kopieren und ausfüllen (Zugangsdaten,
   MQTT-Broker).
2. `docker compose up -d --build`
3. In Home Assistant sollten nach dem ersten Durchlauf (Standard:
   alle 5 Minuten, siehe `POLL_INTERVAL_SECONDS`) automatisch neue Geräte
   unter "Paradigma Pelletsheizung" auftauchen (MQTT-Integration muss in HA
   bereits eingerichtet sein).

## Testen ohne MQTT

```bash
docker compose run --rm paradigma-scraper python paradigma_scraper.py --dry-run
```

Gibt die gelesenen Werte auf der Konsole aus, statt sie zu publishen –
gut zum Prüfen, ob Login und Scraping funktionieren, bevor der Dauerbetrieb
losgeht.

## Bekannte Einschränkungen / Wartungshinweise

- Das Portal blockt laut `robots.txt` automatisierten Zugriff. Das Skript ist
  für den **eigenen Account mit moderatem Abfrageintervall** gedacht – bitte
  `POLL_INTERVAL_SECONDS` nicht zu niedrig setzen (Default 5 Min. ist schon
  knapp, da die Anlage selbst nur ca. minütlich neue Werte liefert).
- Der Login-Schritt sucht generisch nach dem ersten Passwortfeld auf der
  Seite und dem Textfeld direkt davor. Ändert Paradigma das Login-Formular
  grundlegend, muss das angepasst werden (siehe `login()` im Skript).
- Ändert sich die Tabellenstruktur (Klassen `elementList`, `title right`,
  `number`, `center`), muss `scrape_values()` angepasst werden. 
- Nur ein Client kann gleichzeitig eingeloggt sein wie bei den meisten
  Session-basierten Logins üblich – falls du dich parallel im Browser
  einloggst, kann das die Skript-Session invalidieren und umgekehrt.
