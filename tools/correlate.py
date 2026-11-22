#!/usr/bin/env python3
"""Ordnet unbekannte Bits/Bytes der Monitoring-Frames den Werten des alten
Portal-Scrapers zu.

Eingabe: raw.log der Bridge (RAW_LOG_FILE + CORRELATE_TOPIC), Zeilen
  <epoch> RAW <block> <hex>      z.B. 0c01, 0c02, 2401
  <epoch> MQTT <topic> <wert>
Vorgehen: alle STEP Sekunden wird der jeweils letzte Stand beider Quellen
verglichen. Fuer jedes Scraper-Topic mit wenigen Zustaenden (an/aus, Status-
texte) wird jedes Bit jedes Bytes gegen den Zustand getestet; fuer numerische
Topics werden int16 (BE/LE, /1 und /10) und Einzelbytes gegen den Wert
getestet. Ausgegeben werden die besten Kandidaten mit Trefferquote.

Hinweis: der Scraper liefert nur alle 5 min und mit Verzoegerung, daher sind
~90 % Trefferquote schon ein starkes Signal. Aussagekraeftig erst, wenn der
Zustand mehrfach gewechselt hat (Spalte "Wechsel").

Aufruf: python correlate.py raw.log [--step 60] [--topic brenner]
"""
import argparse
import collections
import struct

ap = argparse.ArgumentParser()
ap.add_argument("log")
ap.add_argument("--step", type=int, default=60)
ap.add_argument("--topic", default="", help="nur Topics mit diesem Teilstring")
ap.add_argument("--top", type=int, default=3)
a = ap.parse_args()

events = []
for line in open(a.log, encoding="utf-8", errors="replace"):
    p = line.rstrip("\n").split(" ", 3)
    if len(p) == 4:
        events.append((int(p[0]), p[1], p[2], p[3]))
events.sort()
if not events:
    raise SystemExit("leeres Log")

# Zeitraster mit letztem bekannten Stand
raw_state, mqtt_state, samples = {}, {}, []
i, t = 0, events[0][0]
while t <= events[-1][0]:
    while i < len(events) and events[i][0] <= t:
        _, kind, key, val = events[i]
        (raw_state if kind == "RAW" else mqtt_state)[key] = val
        i += 1
    if raw_state and mqtt_state:
        samples.append(({k: bytes.fromhex(v) for k, v in raw_state.items()}, dict(mqtt_state)))
    t += a.step
print(f"{len(events)} Ereignisse, {len(samples)} Stichproben à {a.step}s\n")


def num(v):
    try:
        return float(v.replace(",", "."))
    except ValueError:
        return None


topics = sorted({k for _, m in samples for k in m if a.topic in k})
for topic in topics:
    vals = [m.get(topic) for _, m in samples]
    present = [v for v in vals if v is not None]
    distinct = collections.Counter(present)
    changes = sum(1 for x, y in zip(present, present[1:]) if x != y)
    numeric = all(num(v) is not None for v in distinct) and len(distinct) > 4
    cands = []
    for blk in sorted({b for r, _ in samples for b in r}):
        n = max(len(r.get(blk, b"")) for r, _ in samples)
        if numeric:
            for off in range(n - 1):
                for fmt, lab in ((">h", "BE"), ("<h", "LE")):
                    for div in (1, 10):
                        ok = tot = 0
                        for r, m in samples:
                            d, v = r.get(blk), num(m.get(topic) or "x")
                            if d is None or v is None or off + 2 > len(d):
                                continue
                            tot += 1
                            ok += abs(struct.unpack_from(fmt, d, off)[0] / div - v) <= max(0.6, abs(v) * 0.03)
                        if tot:
                            cands.append((ok / tot, f"{blk}@{off} int16{lab}/{div}"))
        elif len(distinct) >= 2:
            # binaere Zuordnung: welcher Zustand <-> Bit gesetzt
            for off in range(n):
                for bit in range(8):
                    pairs = [((r[blk][off] >> bit) & 1, m.get(topic)) for r, m in samples
                             if blk in r and off < len(r[blk]) and m.get(topic) is not None]
                    if not pairs:
                        continue
                    table = collections.Counter(pairs)
                    # beste Abbildung Bit->Zustand
                    best = sum(max((c for (b, s), c in table.items() if b == bv), default=0) for bv in (0, 1))
                    bits_seen = {b for b, _ in pairs}
                    if len(bits_seen) == 2:
                        cands.append((best / len(pairs), f"{blk}@{off} bit{bit}"))
            for off in range(n):  # ganzes Byte als Code
                pairs = [(r[blk][off], m.get(topic)) for r, m in samples
                         if blk in r and off < len(r[blk]) and m.get(topic) is not None]
                codes = collections.defaultdict(collections.Counter)
                for b, s in pairs:
                    codes[b][s] += 1
                if pairs and 1 < len(codes) <= 16:
                    best = sum(c.most_common(1)[0][1] for c in codes.values())
                    cands.append((best / len(pairs) - 0.001, f"{blk}@{off} byte-code"))
    cands.sort(reverse=True)
    top = ", ".join(f"{s:.0%} {c}" for s, c in cands[: a.top]) or "-"
    print(f"{topic}  [Zustände {len(distinct)}, Wechsel {changes}]\n    {top}")
