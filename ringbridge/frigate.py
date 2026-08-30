"""
Frigate-Anbindung: Ring-Beschreibung ins Frigate-Ereignis schreiben.

Ring liefert im Push einen von seinem LLM geschriebenen Satz. Frigate
kennt ein Beschreibungsfeld je Ereignis (`POST /api/events/<id>/
description`) und indexiert es bei aktivem `semantic_search` fuer die
Suche. Beides zusammenzubringen macht die Ring-Beschreibung in Frigate
auffindbar.

Zuordnung ueber die Zeit: Nach dem Einspielen eines Clips entsteht in
Frigate wenige Sekunden spaeter ein Ereignis auf derselben Kamera. Wir
suchen das erste Ereignis, das *nach* dem Einspielen begonnen hat.

Das ist eine Heuristik, keine exakte Verknuepfung — Frigate und Ring
kennen einander nicht. Bleibt ein Treffer aus, passiert nichts weiter:
die Beschreibung steht ohnehin auch auf MQTT.

⚠️ STANDARDMAESSIG AUS (frigate.enabled = false), aus zwei Gruenden:

1. Frigate verwirft die Beschreibung still. Ohne aktivierte
   GenAI-Funktion antwortet `POST /api/events/<id>/description` mit
   HTTP 200 und {"success": true}, speichert aber nichts — nachgeprueft
   am 2026-08-30 gegen Frigate 0.17.2: zurueckgelesen kommt `None`, und
   die Suche findet den Text nicht. `object_descriptions` steht bei
   allen Kameras auf OFF, und `cameras.<name>.genai` ist gar nicht
   gesetzt; der MQTT-Schalter greift deshalb auch nicht.
   Vor dem Einschalten also erst in Frigate GenAI einrichten
   (Provider + Schluessel, pro Kamera aktiv) — dann erzeugt Frigate
   allerdings auch eigene Beschreibungen und ueberschreibt unsere
   moeglicherweise.

2. **Bekannter Fehler, noch nicht behoben:** Es gibt keine Verknuepfung
   zwischen einem Push und dem Clip, der spaeter eingespielt wird.
   `remember()` bekommt einfach die zuletzt gesehenen Push-Werte. Kommt
   der Push spaeter als der Clip (am 2026-08-30 beobachtet: Clip
   14:27:14, Schreibvorgang 14:27:38, zugehoeriger Push erst 14:27:47),
   wird die Beschreibung des VORIGEN Ereignisses geschrieben.
   Vor einer Reaktivierung muss die Zuordnung ueber die Aufnahme-ID
   oder den Ereigniszeitpunkt laufen, nicht ueber "zuletzt gesehen".
"""

import json
import logging
import time
import urllib.error
import urllib.request

from ringbridge.config import *


log = logging.getLogger(__name__)

# Wie lange nach dem Einspielen auf ein passendes Frigate-Ereignis
# gewartet wird, bevor aufgegeben wird.
DEFAULT_MATCH_WINDOW = 90
# Wieviel frueher als der Einspielzeitpunkt ein Ereignis noch zaehlt.
# Frigate kann die Erkennung leicht vordatieren.
MATCH_BACKDATE = 10


class FrigateAnnotator:
    def __init__(self):
        cfg = CONFIG.get('frigate') or {}
        self.enabled = bool(cfg.get('enabled'))
        self.url = (cfg.get('url') or '').rstrip('/')
        self.window = int(cfg.get('match_window_seconds', DEFAULT_MATCH_WINDOW))
        self.set_sub_label = bool(cfg.get('set_sub_label', False))
        # camera -> {'since': float, 'values': dict, 'deadline': float}
        self._pending = {}

        if self.enabled and not self.url:
            log.warning("frigate.enabled ist true, aber frigate.url fehlt")
            self.enabled = False

    def remember(self, camera_key: str, values: dict) -> None:
        """Nach dem Einspielen eines Clips: auf das Frigate-Ereignis warten."""
        if not self.enabled or not values.get('description'):
            return

        now = time.time()
        self._pending[camera_key] = {
            'since': now - MATCH_BACKDATE,
            'values': values,
            'deadline': now + self.window,
        }
        log.debug(f"frigate: warte auf Ereignis fuer {camera_key}")

    def process(self) -> None:
        """Bei jedem Schleifendurchlauf aufrufen; schluckt alle Fehler."""
        if not self.enabled or not self._pending:
            return

        now = time.time()
        for camera_key in list(self._pending):
            entry = self._pending[camera_key]

            if now > entry['deadline']:
                log.info(f"frigate: kein Ereignis fuer {camera_key} innerhalb "
                         f"{self.window}s - Beschreibung nicht zugeordnet")
                self._pending.pop(camera_key, None)
                continue

            try:
                if self._try_annotate(camera_key, entry):
                    self._pending.pop(camera_key, None)
            except Exception as e:
                log.debug(f"frigate: Zuordnung fuer {camera_key} fehlgeschlagen: {e}")

    # ------------------------------------------------------------------

    def _get(self, path: str):
        req = urllib.request.Request(f"{self.url}{path}")
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())

    def _post(self, path: str, payload: dict) -> int:
        data = json.dumps(payload).encode()
        req = urllib.request.Request(f"{self.url}{path}", data=data, method='POST',
                                     headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status

    def _try_annotate(self, camera_key: str, entry: dict) -> bool:
        events = self._get(f"/api/events?camera={camera_key}&limit=5")

        # Aeltestes passendes Ereignis nach dem Einspielzeitpunkt.
        candidates = [e for e in events
                      if e.get('start_time', 0) >= entry['since']]
        if not candidates:
            return False

        event = min(candidates, key=lambda e: e['start_time'])
        event_id = event['id']
        description = entry['values']['description']

        self._post(f"/api/events/{event_id}/description",
                   {"description": description})

        if self.set_sub_label and entry['values'].get('detection'):
            try:
                self._post(f"/api/events/{event_id}/sub_label",
                           {"subLabel": entry['values']['detection']})
            except Exception as e:
                log.debug(f"frigate: sub_label fehlgeschlagen: {e}")

        log.info(f"frigate: Beschreibung an Ereignis {event_id} "
                 f"({camera_key}) geschrieben")
        return True
