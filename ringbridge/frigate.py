"""
Frigate integration: write the Ring description into the Frigate event.

Ring's push carries a sentence written by its LLM. Frigate has a
description field per event (`POST /api/events/<id>/description`) and
indexes it for search when `semantic_search` is enabled. Bringing the two
together makes the Ring description findable inside Frigate.

Matching is done over time: a few seconds after a clip is spliced in,
Frigate creates an event on the same camera. We look for the first event
that started *after* the splice.

That is a heuristic, not a real link - Frigate and Ring know nothing about
each other. If no match turns up, nothing bad happens: the description is
on MQTT anyway.

⚠️ OFF BY DEFAULT (frigate.enabled = false), for two reasons:

1. Frigate discards the description silently. Without its GenAI feature
   enabled, `POST /api/events/<id>/description` answers with HTTP 200 and
   {"success": true} but stores nothing - verified on 2026-08-30 against
   Frigate 0.17.2: reading it back returns `None`, and search does not
   find the text. `object_descriptions` was OFF on every camera and
   `cameras.<name>.genai` was not set at all, which is also why the MQTT
   switch for it had no effect.
   So before enabling this, set up GenAI in Frigate first (provider +
   key, enabled per camera) - at which point Frigate will also generate
   its own descriptions and may overwrite ours.

2. **Known bug, not yet fixed:** there is no link between a push and the
   clip that gets spliced in later. `remember()` simply receives the most
   recently seen push values. If the push arrives later than the clip
   (observed on 2026-08-30: clip 14:27:14, write 14:27:38, matching push
   only at 14:27:47), the description of the PREVIOUS event is written.
   Before re-enabling this, matching has to go through the recording ID or
   the event timestamp, not through "most recently seen".
"""

import json
import logging
import time
import urllib.error
import urllib.request

from ringbridge.config import *


log = logging.getLogger(__name__)

# How long to keep waiting for a matching Frigate event after a splice
# before giving up.
DEFAULT_MATCH_WINDOW = 90
# How much earlier than the splice an event may still count. Frigate can
# date its detection slightly ahead.
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
            log.warning("frigate.enabled is true but frigate.url is missing")
            self.enabled = False

    def remember(self, camera_key: str, values: dict) -> None:
        """After splicing a clip: start waiting for the Frigate event."""
        if not self.enabled or not values.get('description'):
            return

        now = time.time()
        self._pending[camera_key] = {
            'since': now - MATCH_BACKDATE,
            'values': values,
            'deadline': now + self.window,
        }
        log.debug(f"frigate: waiting for an event for {camera_key}")

    def process(self) -> None:
        """Call on every loop tick; swallows all errors."""
        if not self.enabled or not self._pending:
            return

        now = time.time()
        for camera_key in list(self._pending):
            entry = self._pending[camera_key]

            if now > entry['deadline']:
                log.info(f"frigate: no event for {camera_key} within "
                         f"{self.window}s - description not attached")
                self._pending.pop(camera_key, None)
                continue

            try:
                if self._try_annotate(camera_key, entry):
                    self._pending.pop(camera_key, None)
            except Exception as e:
                log.debug(f"frigate: matching for {camera_key} failed: {e}")

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

        # Oldest matching event after the splice point.
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
                log.debug(f"frigate: sub_label failed: {e}")

        log.info(f"frigate: description written to event {event_id} "
                 f"({camera_key})")
        return True
