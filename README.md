# Paradigma SystaComfort II → MQTT / Home Assistant (lokal, ohne Modbus)

Liest die Messwerte einer **Paradigma SystaComfort II ohne Modbus-Schnittstelle**
direkt im lokalen Netz aus und stellt sie per MQTT (mit Home-Assistant-Discovery)
bereit. Kein Fernwartungsportal, kein Abo, keine DNS-Umleitung, keine Änderung
am Regler.

Hintergrund: Ein Teil der SystaComfort-II-Regler wurde (u. a. wegen
Bauteilmangel um 2021/2022) ohne Modbus ausgeliefert – nur mit einem LAN-Port.
Die Daten an das Paradigma-Portal sind dort verschlüsselt. Der Regler bietet
aber eine lokale, unverschlüsselte Service-Schnittstelle auf **UDP-Port 7260**,
die auch die Wartungssoftware *SystaService* für ihr Monitoring nutzt. Diese
Bridge spricht genau dieses Protokoll – **nur lesend**.

## Werte

Alle ~10 Sekunden:

| SystaComfort II | Solarregler (SystaSolar Aqua II) |
|---|---|
| Außentemperatur, Warmwasser + Soll | Kollektor TSA |
| Puffer oben / unten + Soll | Speicher oben TW |
| HK1/HK2 Vorlauf, Rücklauf, Raum + Sollwerte | Solar Vorlauf / Rücklauf |
| Kessel Vorlauf / Rücklauf | Außenfühler TAM |
| Pumpen PK / PHK / PHK2 (%) | Tagesgewinn, Gesamtgewinn (kWh) |
| Kessel-Betriebsstunden, Kesselstarts | |
| Kessel-Anforderung (B1), Kesselpumpe aktiv (an/aus) | |

Nicht angeschlossene Fühler (Rohwert −30,0 °C) werden ausgelassen.
MQTT-Topics: `systa/<wert>`, Verfügbarkeit `systa/status`, HA-Gerät
„SystaComfort II (lokal)“.

**Noch nicht entschlüsselt:** weitere Relais (Mischer, Zirkulation, Solarpumpe),
Betriebsart/Status-Texte, Störungscodes, Solarleistung, Volumenstrom.
Beiträge willkommen – siehe [Weitere Felder entschlüsseln](#weitere-felder-entschlüsseln).

## Voraussetzungen

- SystaComfort II mit LAN-Anschluss im selben Netz (getestet: Firmware V1.34.1)
- IP-Adresse des Reglers (z. B. aus dem Router; der Regler antwortet auch auf
  die Gerätesuche, siehe unten)
- UDP-Passwort des Reglers – Werkseinstellung `1234`
- MQTT-Broker (z. B. Mosquitto-Add-on in Home Assistant)

## Installation (Docker)

```bash
git clone https://github.com/mpetereit/Paradigma_SystaComfort_II.git
cd Paradigma_SystaComfort_II
cp .env.example .env      # SYSTA_HOST und MQTT-Daten eintragen
docker compose up -d --build
docker logs -f systa-bridge
```

Ohne Docker: `pip install paho-mqtt==1.6.1` und
`SYSTA_HOST=<IP> python systa_bridge.py` (ohne `MQTT_HOST` werden die Werte nur
geloggt – gut zum Testen).

## Protokoll (Kurzfassung)

**Gerätesuche / Info – UDP 8001, ASCII:**
`0 1 A` (Broadcast oder direkt) → `SC2 1 <IP> <Maske> <Gateway> SystaComfort-II …`
`<MAC> 6 R UDP Port` → `0 7 7260`, `<MAC> 6 R UDP Pass` → `0 7 1234`

**Service-Schnittstelle – UDP 7260, binär:**

```
Anfrage : "<UDP-Passwort>\n" [Len] [Cmd ...] [CS]
Antwort : [4 Byte Zeitstempel] [Typ] [Len] [Nutzdaten ...] [CS]
          Typ 0x0a = Echo, 0xfc = Monitoring-Daten, 0xfd = Antwort, 0xfa = Bus
Prüfsumme: Summe aller Frame-Bytes ≡ 0 (mod 256)
```

| Befehl | Bedeutung |
|---|---|
| `16` | Bus-Monitor an |
| `17` | Monitoring SystaComfort an |
| `1c 0c 18` | (Teil der Start-Sequenz von SystaService) |
| `14` | Monitoring Solarregler an |
| `1c 0c 03 <AdrH> <AdrL> <Len>` | Speicher/Parameter lesen („Alles Auslesen“) |

Nach der Start-Sequenz sendet der Regler alle ~10 s; ohne Wiederholung endet
der Strom nach ~60 s (die Bridge wiederholt alle 30 s).

| Block | Inhalt | Kodierung |
|---|---|---|
| `fc 29 0c 01` | Messwerte SystaComfort (ab Byte 4: TA, TWO, FA TV, FA TR, TI, TI2, TV, TV2, TR, TR2, TPO, TPU, TZR) | int16 big endian, /10 °C |
| `fc 29 0c 02` | Sollwerte, Pumpen-%, Betriebsstunden (u32 @14), Kesselstarts (u32 @18) | big endian |
| `fc 3e 24 01` | Solarregler (TSA, TW, TSV, TAM, –, TSE); Tagesgewinn u16 @27 /10 kWh, Gesamtgewinn u16 @33 kWh | little endian |

Vollständige Feldtabelle: `systa_bridge.py`.

> ⚠️ Schreibbefehle sind nicht dokumentiert und werden von dieser Bridge
> bewusst nicht verwendet. Experimente mit unbekannten Befehlscodes können
> Einstellungen am Regler verändern.

## Weitere Felder entschlüsseln

Mit `RAW_LOG_FILE=/data/raw.log` schreibt die Bridge jeden geänderten
Rohframe mit Zeitstempel mit. Ist zusätzlich `CORRELATE_TOPIC` gesetzt (z. B.
auf die MQTT-Topics einer anderen Datenquelle mit bekannten Werten), werden
auch diese Werte geloggt. `tools/correlate.py raw.log` sucht dann zu jedem
bekannten Wert das passende Bit/Byte im Rohframe.

## Älterer Ansatz: Portal-Scraper

Im Ordner [`scraper/`](scraper/) liegt ein Playwright-Scraper, der die Werte
über das kostenpflichtige Paradigma-Fernwartungsportal ausliest. Er wird für
die lokale Bridge nicht benötigt.

## Haftungsausschluss

Inoffizielles Projekt, nicht mit Paradigma / Ritter Energie- und
Umwelttechnik verbunden. Nutzung auf eigene Gefahr. Die Bridge sendet
ausschließlich die Lese-/Monitoring-Befehle, die auch die offizielle
Wartungssoftware verwendet.

## Lizenz

[MIT](LICENSE)
