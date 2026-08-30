"""
Ring-Anbindung fuer ringbridge.

Portierung von blinkbridges `blink.py` auf die Ring-Cloud. Bedient dieselbe
CameraManager-Schnittstelle, die `main.py` erwartet:

    await start()                              Anmeldung + erste Metadaten
    get_cameras()                              Namen der Kameras
    await save_latest_clip(name, force=False)  letzten Clip laden -> Pfad
    await check_for_motion(name)               neuer Clip? -> Pfad oder None
    await refresh_metadata()
    await close()

Prinzip (wie blinkbridge): Es wird KEIN Live-Stream aus der Ring-Cloud
gezogen. Stattdessen wird nach einem Bewegungsereignis die fertige
Cloud-Aufnahme als MP4 heruntergeladen. Zwischen den Ereignissen laeuft
lokal eine Standbildschleife. Das ist der Grund, warum Ring seine
Motion-Events weiter zustellt: es gibt keine dauerhafte Live-Session.

Erfordert ein Ring-Protect-Abo, da nur dann Cloud-Aufnahmen existieren.

Getestet gegen ring_doorbell 0.9.14.
"""

import asyncio
import json
import logging
import os
import re
import subprocess
import time
import unicodedata
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Union

from aiohttp import ClientSession
from ring_doorbell import (Auth, Ring, Requires2FAError, AuthenticationError,
                           RingEventListener)

from ringbridge.config import *
from ringbridge.mqtt import MqttPublisher
from ringbridge.frigate import FrigateAnnotator


log = logging.getLogger(__name__)

USER_AGENT = "ringbridge/0.1"
CRED_FILE = ".ring_token.json"

# Wie viele History-Eintraege nach einer fertigen Aufnahme durchsucht werden.
HISTORY_LIMIT = 10
# Frische Aufnahmen sind kurz nach dem Ereignis noch nicht abrufbar,
# obwohl die History sie als "ready" fuehrt -> nachfassen.
DOWNLOAD_ATTEMPTS = 3
DOWNLOAD_RETRY_DELAY = 5

# Welche Ereignisarten ueberhaupt in Frage kommen. "on_demand" bewusst
# NICHT: das sind Live-View-Sitzungen, also der eigene Zugriff.
DEFAULT_EVENT_KINDS = ['motion', 'ding']
# Notbremse gegen Ausreisser (Ring liefert on_demand-Clips von 10+ Minuten).
DEFAULT_MAX_CLIP_SECONDS = 180

# Umcodierung je Kamera (ring.transcode). Gedacht fuer Ring-Kameras, die
# HEVC liefern: Clip und Standbild kommen sonst aus verschiedenen
# HEVC-Encodern (Ring bzw. libx265), deren Parametersaetze sich
# unterscheiden - beim Schnitt per "copy" gibt das gelegentlich kaputte
# Bilder. Nach H.264 umcodiert stammen beide aus derselben Familie und das
# Problem verschwindet (blink2 laeuft mit 2560x1440 H.264 fehlerfrei).
# Nebeneffekt: das Standbild ist danach in 0,3 s statt 4,7 s erzeugt.
TRANSCODE_PRESET = 'ultrafast'

# Push-Kanal (FCM). Nach einem Push wird fuer diese Dauer bei jedem
# Schleifendurchlauf nach der fertigen Aufnahme gefragt - Ring braucht
# nach dem Ereignis noch Zeit zum Transkodieren.
PUSH_WINDOW_SECONDS = 300
# Ohne Push wird nur alle so vielen Sekunden ueberhaupt die History
# abgefragt. Der Schleifentakt (poll_interval) darf dadurch klein
# bleiben, ohne die Ring-API zu belasten.
DEFAULT_IDLE_POLL_SECONDS = 120
PUSH_CRED_FILE = ".ring_push.json"
# Die FCM-Registrierung bei Google scheitert sporadisch mit
# PHONE_REGISTRATION_ERROR und klappt beim naechsten Anlauf. Deshalb
# mehrfach versuchen; sobald sie einmal sitzt, liegen die Credentials
# auf Platte und kuenftige Starts registrieren gar nicht mehr neu.
LISTENER_START_ATTEMPTS = 4
LISTENER_RETRY_DELAY = 10


def sanitize(name: str) -> str:
    """Kameraname -> Dateinamensbestandteil (identisch zu stream_server)."""
    n = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode()
    n = re.sub(r'[^A-Za-z0-9]+', '_', n).strip('_').lower()
    return n or 'camera'


class CameraManager:
    def __init__(self):
        self.session = ClientSession()
        self.camera_last_record = defaultdict(lambda: None)
        self.ring = None
        self._cred_path = None
        self._hardware_id = None
        self.listener = None
        self.mqtt = MqttPublisher()
        self.frigate = FrigateAnnotator()
        # Letzte Push-Angaben je Kamera, fuer die Frigate-Zuordnung.
        self._last_push_values = {}
        # Zeitpunkt des letzten Push je Kamera bzw. der letzten History-Abfrage.
        self._push_at = {}
        self._last_history_check = defaultdict(float)

    # ------------------------------------------------------------------ auth

    def _save_token(self, token: dict) -> None:
        """token_updater-Callback: Ring erneuert Tokens im Betrieb."""
        try:
            self._cred_path.write_text(json.dumps({
                "token": token,
                "hardware_id": self._hardware_id,
            }))
            self._cred_path.chmod(0o600)
            log.debug("Ring-Token gespeichert")
        except Exception as e:
            log.error(f"Konnte Ring-Token nicht speichern: {e}")

    async def _login(self) -> None:
        self._cred_path = PATH_CONFIG / CRED_FILE

        token = None
        if self._cred_path.exists():
            try:
                saved = json.loads(self._cred_path.read_text())
                token = saved.get("token")
                self._hardware_id = saved.get("hardware_id")
                log.info("Logging into Ring with saved token")
            except Exception as e:
                log.warning(f"Gespeicherte Ring-Credentials unlesbar ({e}), neu anmelden")
                token = None

        # Stabile hardware_id: sonst sieht Ring bei jedem Start ein neues
        # Geraet und verlangt erneut 2FA.
        if not self._hardware_id:
            self._hardware_id = str(uuid.uuid4())

        auth = Auth(USER_AGENT, token, self._save_token,
                    hardware_id=self._hardware_id,
                    http_client_session=self.session)

        if token is None:
            log.info("Logging into Ring with credentials from config")
            login = CONFIG['ring']['login']
            try:
                token = await auth.async_fetch_token(login['username'], login['password'])
            except Requires2FAError:
                log.info("Two-factor authentication required")
                code = input("Enter your Ring 2FA code: ").strip()
                token = await auth.async_fetch_token(
                    login['username'], login['password'], code)
                log.info("2FA accepted")
            self._save_token(token)

        self.ring = Ring(auth)

        try:
            await self.ring.async_update_data()
        except AuthenticationError as e:
            # Abgelaufenes/entwertetes Token: wegwerfen, damit der naechste
            # Start wieder den Passwort-Weg (inkl. 2FA) nimmt.
            log.error(f"Ring-Authentifizierung fehlgeschlagen: {e}")
            if self._cred_path.exists():
                log.info("Entferne ungueltiges Ring-Token")
                self._cred_path.unlink()
            raise

        log.info("Successfully authenticated with Ring")

    # ------------------------------------------------------------------ push

    def _on_ring_event(self, event) -> None:
        """
        Callback des FCM-Listeners. Laeuft in einem fremden Thread, deshalb
        hier nur einen Zeitstempel setzen und nichts Aufwendiges tun.
        """
        # Rohereignis mitloggen: RingEvent traegt laut Bibliothek keine
        # Beschreibungstexte ("Eine Katze sitzt auf der Fensterbank"), die
        # in der Ring-App erscheinen. Falls doch etwas mitkommt, steht es hier.
        log.info(f"push: {event.device_name} kind={event.kind} state={event.state} "
                 f"id={event.id} update={event.is_update}")
        log.debug(f"push roh: {vars(event)}")

        kinds = CONFIG['ring'].get('event_kinds', DEFAULT_EVENT_KINDS)
        if event.kind not in kinds:
            return

        self._push_at[event.device_name] = time.time()

    def _wrap_event_parser(self) -> None:
        """
        Schutzhuelle um `RingEventListener._get_ring_event`.

        Ring verschickt Nachrichtenformate, die die Bibliothek nicht kennt.
        Beobachtet: `KeyError: 'id'` in `_get_ring_event`, weil
        `data.event.ding.id` fehlt. Die Ausnahme fliegt bis in den
        FCM-Client ("Unexpected exception calling notification callback")
        und das Ereignis geht verloren.

        Hier wird die Rohnachricht geloggt (dort koennten die
        Beschreibungstexte der Ring-App stecken) und - wichtiger - der
        Gerätename herausgezogen, damit auch eine unverstandene Nachricht
        als Ausloeser taugt.

        Greift auf eine private Methode zu; bei einem Update von
        ring_doorbell hier zuerst nachsehen.
        """
        original = self.listener._get_ring_event

        def wrapped(msg_data):
            try:
                return original(msg_data)
            except Exception as e:
                # Kein Fehlerfall: ring_doorbell versteht dieses Format
                # nicht (data.event.ding hat kein "id"), wir aber schon -
                # und es sind gerade die Nachrichten MIT Beschreibungstext.
                # Deshalb nur DEBUG; der Erfolgsfall loggt weiter unten
                # eine verstaendliche INFO-Zeile.
                log.debug(f"push: von ring_doorbell nicht geparst "
                          f"({type(e).__name__}: {e}) - wird selbst ausgewertet")
                try:
                    log.debug("push roh: " + json.dumps(msg_data, ensure_ascii=False)[:2000])
                except Exception:
                    log.debug(f"push roh (unserialisierbar): {msg_data}")

                # Trotzdem verwerten: Ausloeser setzen und die reichen
                # Felder nach MQTT geben (Beschreibung, Klassifikation,
                # Snapshot) - genau die gibt es nur hier, nicht in der
                # History-API.
                try:
                    self._handle_rich_push(msg_data)
                except Exception as e:
                    # Jetzt ist es wirklich ein Fehler: weder Bibliothek
                    # noch wir konnten etwas damit anfangen.
                    log.warning(f"push: Nachricht nicht verwertbar ({e})")
                    log.warning("push roh: " + str(msg_data)[:1000])
                return None

        self.listener._get_ring_event = wrapped

    def _handle_rich_push(self, msg_data: dict) -> None:
        """
        Die Felder aus einer Ring-Push-Nachricht auswerten.

        Aufbau (gemessen 2026-08-30):
          android_config -> title, body   (body = der LLM-Satz)
          data.device    -> name, id, kind
          data.event.ding-> created_at, subtype, detection_type
          img            -> snapshot_url
        """
        data = json.loads(msg_data.get('data', '{}'))
        android = json.loads(msg_data.get('android_config', '{}'))
        img = json.loads(msg_data.get('img', '{}'))

        name = (data.get('device') or {}).get('name')
        if not name:
            return

        self._push_at[name] = time.time()

        ding = ((data.get('event') or {}).get('ding')) or {}
        values = {
            'title': android.get('title'),
            'description': android.get('body'),
            'detection': ding.get('detection_type') or ding.get('subtype'),
            'timestamp': ding.get('created_at'),
            'snapshot_url': img.get('snapshot_url'),
            'camera': name,
        }

        if values['description']:
            log.info(f"push: {name} [{values['detection']}] {values['description']}")
        else:
            log.info(f"push: {name} [{values.get('detection') or '?'}] (ohne Beschreibung)")

        # Frigate-Kameranamen koennen von den Ring-Namen abweichen
        # (hier: "CAT Cam" heisst in Frigate "camera_c"). Ueber
        # ring.camera_names uebersetzt, damit die MQTT-Themen zu den
        # Frigate-Kameras passen und HA beides zusammenbringt.
        mapping = CONFIG['ring'].get('camera_names') or {}
        key = mapping.get(name) or sanitize(name)

        self._last_push_values[key] = values
        self.mqtt.publish_event(key, name, values)

    async def _start_listener(self) -> None:
        """
        FCM-Push abonnieren. Ergaenzung, kein Ersatz: schlaegt der Push fehl,
        laeuft das History-Polling als Sicherheitsnetz weiter.
        """
        push_path = PATH_CONFIG / PUSH_CRED_FILE

        credentials = None
        if push_path.exists():
            try:
                credentials = json.loads(push_path.read_text())
            except Exception as e:
                log.warning(f"Push-Credentials unlesbar ({e}), werden neu geholt")

        def save_credentials(creds):
            try:
                push_path.write_text(json.dumps(creds))
                push_path.chmod(0o600)
            except Exception as e:
                log.error(f"Konnte Push-Credentials nicht speichern: {e}")

        for attempt in range(1, LISTENER_START_ATTEMPTS + 1):
            try:
                self.listener = RingEventListener(
                    self.ring, credentials, save_credentials)
                if await self.listener.start(timeout=20):
                    self._wrap_event_parser()
                    self.listener.add_notification_callback(self._on_ring_event)
                    log.info(f"Push-Kanal (FCM) aktiv (Versuch {attempt})")
                    return
                log.warning(f"Push-Kanal: Start fehlgeschlagen "
                            f"(Versuch {attempt}/{LISTENER_START_ATTEMPTS})")
            except Exception as e:
                log.warning(f"Push-Kanal: Fehler bei Versuch "
                            f"{attempt}/{LISTENER_START_ATTEMPTS}: {e}")

            self.listener = None
            if attempt < LISTENER_START_ATTEMPTS:
                await asyncio.sleep(LISTENER_RETRY_DELAY)

        log.warning("Push-Kanal nicht verfuegbar - es wird weiter gepollt "
                    "(Sicherheitsnetz, hoehere Latenz)")

    def _should_check(self, camera_name: str) -> bool:
        """History abfragen? Nach einem Push ja, sonst nur im Leerlauftakt."""
        now = time.time()

        pushed_at = self._push_at.get(camera_name)
        if pushed_at and now - pushed_at < PUSH_WINDOW_SECONDS:
            return True

        idle = CONFIG['ring'].get('idle_poll_seconds', DEFAULT_IDLE_POLL_SECONDS)
        if now - self._last_history_check[camera_name] >= idle:
            return True

        return False

    # --------------------------------------------------------------- helpers

    def _device(self, camera_name: str):
        dev = self.ring.get_video_device_by_name(camera_name)
        if dev is None:
            log.warning(f"{camera_name}: Kamera nicht (mehr) in der Ring-Liste")
        return dev

    def _clip_path(self, camera_name: str) -> Path:
        return PATH_VIDEOS / f"{sanitize(camera_name)}_latest.mp4"

    async def _last_ready_recording_id(self, dev) -> Union[int, None]:
        """
        Neueste brauchbare Aufnahme.

        Drei Filter, jeder aus einem konkreten Fehlschlag entstanden:

        1. `recording.status == 'ready'` — `async_get_last_recording_id()`
           der Bibliothek nimmt blind den letzten History-Eintrag.
        2. `kind` in `event_kinds` — Ring fuehrt auch **`on_demand`**-
           Aufnahmen, und das sind die eigenen Live-View-Sitzungen. Wer die
           einspielt, zeigt Frigate stundenaltes Material aus dem eigenen
           Zugriff (hier gemessen: ein 849-s-Clip mit 231 MB).
        3. `duration <= max_clip_seconds` — Notbremse gegen Ausreisser.
        """
        try:
            history = await dev.async_history(limit=HISTORY_LIMIT)
        except Exception as e:
            log.error(f"{dev.name}: History-Abfrage fehlgeschlagen: {e}")
            return None

        kinds = CONFIG['ring'].get('event_kinds', DEFAULT_EVENT_KINDS)
        max_seconds = CONFIG['ring'].get('max_clip_seconds', DEFAULT_MAX_CLIP_SECONDS)

        for entry in history:
            recording = entry.get('recording') or {}
            if recording.get('status') != 'ready':
                continue

            kind = entry.get('kind')
            if kind not in kinds:
                log.debug(f"{dev.name}: ueberspringe {entry.get('id')} (kind={kind})")
                continue

            duration = entry.get('duration') or 0
            if max_seconds and duration > max_seconds:
                log.warning(f"{dev.name}: ueberspringe {entry.get('id')} "
                            f"(kind={kind}, {duration:.0f}s > {max_seconds}s)")
                continue

            return entry.get('id')

        return None

    async def _download(self, dev, recording_id: int, file_name: Path) -> bool:
        """
        Aufnahme holen.

        Primaerweg ist `async_recording_url()`: das liefert eine signierte
        URL auf Ring's CDN und ist derselbe Weg, den auch das
        Scrypted-Ring-Plugin nutzt. Der Bibliotheks-Direktdownload
        (`/clients_api/dings/<id>/recording`) dient nur als Rueckfallebene —
        er antwortet auf frische Aufnahmen zeitweise mit 404, obwohl die
        History sie bereits als `ready` fuehrt.
        """
        for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
            if attempt > 1:
                log.debug(f"{dev.name}: Aufnahme {recording_id}, Versuch {attempt} "
                          f"in {DOWNLOAD_RETRY_DELAY}s")
                await asyncio.sleep(DOWNLOAD_RETRY_DELAY)

            if await self._try_download(dev, recording_id, file_name):
                # Umcodierung blockiert mehrere Sekunden -> in einen Thread,
                # damit der Event-Loop weiterlaeuft.
                await asyncio.to_thread(self._transcode, dev.name, file_name)
                return True

        log.error(f"{dev.name}: Aufnahme {recording_id} nach {DOWNLOAD_ATTEMPTS} "
                  f"Versuchen nicht ladbar")
        return False

    async def _try_download(self, dev, recording_id: int, file_name: Path) -> bool:
        """
        Laedt in eine Nebendatei und benennt erst am Ende um.

        Wichtig: `file_name` ist die Datei, die der laufende ffmpeg gerade
        liest oder gleich lesen wird. Wer da direkt hineinschreibt, liefert
        ihm einen halb geschriebenen MP4 - im Log als
        "Invalid NAL unit size" / "h264_mp4toannexb filter failed".
        `os.replace()` ist auf demselben Dateisystem atomar; ein ffmpeg, das
        die alte Datei bereits offen hat, liest sie unter POSIX zu Ende.
        """
        tmp_name = file_name.with_suffix(file_name.suffix + '.part')

        # 1) signierte CDN-URL
        try:
            url = await dev.async_recording_url(recording_id)
        except Exception as e:
            log.debug(f"{dev.name}: recording_url fehlgeschlagen: {e}")
            url = None

        if url:
            try:
                async with self.session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.read()
                        tmp_name.write_bytes(data)
                        log.debug(f"{dev.name}: {len(data)} B via CDN-URL -> {file_name}")
                    else:
                        log.debug(f"{dev.name}: CDN-URL antwortete HTTP {resp.status}")
                        data = None
            except Exception as e:
                log.debug(f"{dev.name}: CDN-Download fehlgeschlagen: {e}")
                data = None

            if data and tmp_name.exists() and tmp_name.stat().st_size > 0:
                os.replace(tmp_name, file_name)
                return True

        # 2) Rueckfall: Direktdownload der Bibliothek
        try:
            await dev.async_recording_download(
                recording_id, filename=str(tmp_name), override=True)
        except Exception as e:
            log.debug(f"{dev.name}: Direktdownload fehlgeschlagen: {e}")
            tmp_name.unlink(missing_ok=True)
            return False

        if not tmp_name.exists() or tmp_name.stat().st_size == 0:
            log.debug(f"{dev.name}: Aufnahme {recording_id} kam leer an")
            tmp_name.unlink(missing_ok=True)
            return False

        os.replace(tmp_name, file_name)
        return True

    def _transcode(self, camera_name: str, file_name: Path) -> None:
        """
        Clip nach der vorkonfigurierten Vorgabe umcodieren.

        Ueber eine Nebendatei und os.replace(), damit ein bereits lesender
        ffmpeg nicht auf eine halb geschriebene Datei trifft. Schlaegt die
        Umcodierung fehl, bleibt der Originalclip liegen - lieber ein
        HEVC-Clip als gar keiner.
        """
        spec = (CONFIG['ring'].get('transcode') or {}).get(camera_name)
        if not spec:
            return

        tmp = file_name.with_suffix('.tc.mp4')
        args = ['ffmpeg', '-hide_banner', '-loglevel', 'error', '-y',
                '-i', str(file_name),
                '-c:v', spec.get('codec', 'libx264'),
                '-preset', spec.get('preset', TRANSCODE_PRESET),
                '-pix_fmt', 'yuv420p']

        if spec.get('width') and spec.get('height'):
            args += ['-vf', f"scale={spec['width']}:{spec['height']}"]

        args += ['-c:a', 'copy', str(tmp)]

        started = time.time()
        try:
            subprocess.run(args, check=True, capture_output=True, timeout=180)
        except Exception as e:
            log.error(f"{camera_name}: Umcodierung fehlgeschlagen ({e}) - "
                      f"Originalclip wird verwendet")
            tmp.unlink(missing_ok=True)
            return

        if tmp.exists() and tmp.stat().st_size > 0:
            os.replace(tmp, file_name)
            log.info(f"{camera_name}: Clip umcodiert nach "
                     f"{spec.get('codec','libx264')}"
                     f"{'/' + str(spec['height']) + 'p' if spec.get('height') else ''} "
                     f"in {time.time()-started:.1f}s")
        else:
            log.error(f"{camera_name}: Umcodierung ergab leere Datei - "
                      f"Originalclip wird verwendet")
            tmp.unlink(missing_ok=True)

    # ------------------------------------------------------- CameraManager-API

    async def refresh_metadata(self) -> None:
        log.debug('refreshing device metadata')
        await self.ring.async_update_data()

    def get_cameras(self):
        return [d.name for d in self.ring.video_devices()]

    async def save_latest_clip(self, camera_name: str, force: bool = False) -> Union[Path, None]:
        """Letzte vorhandene Cloud-Aufnahme laden (Startbild fuer den Stream)."""
        file_name = self._clip_path(camera_name)

        if file_name.exists() and not force:
            log.debug(f"{camera_name}: skipping download, {file_name} exists")
            return file_name

        dev = self._device(camera_name)
        if dev is None:
            return None

        recording_id = await self._last_ready_recording_id(dev)
        if recording_id is None:
            log.warning(f"{camera_name}: keine Cloud-Aufnahme vorhanden "
                        f"(Ring-Protect-Abo aktiv?)")
            return None

        if not await self._download(dev, recording_id, file_name):
            return None

        # Ausgangspunkt merken, sonst gilt diese Aufnahme sofort als "neu".
        self.camera_last_record[camera_name] = recording_id

        return file_name

    async def check_for_motion(self, camera_name: str) -> Union[Path, None]:
        """Neue Cloud-Aufnahme? Dann laden und Pfad zurueckgeben."""
        self.frigate.process()

        if not self._should_check(camera_name):
            return None

        dev = self._device(camera_name)
        if dev is None:
            return None

        self._last_history_check[camera_name] = time.time()

        recording_id = await self._last_ready_recording_id(dev)

        if recording_id is None or self.camera_last_record[camera_name] == recording_id:
            return None

        log.debug(f"{camera_name}: neue Aufnahme {recording_id}")

        file_name = self._clip_path(camera_name)

        if not await self._download(dev, recording_id, file_name):
            # Nicht merken - beim naechsten Durchlauf erneut versuchen.
            return None

        self.camera_last_record[camera_name] = recording_id
        # Aufnahme ist da - Push-Fenster fuer diese Kamera schliessen.
        self._push_at.pop(camera_name, None)

        # Frigate erzeugt gleich ein Ereignis aus dem eingespielten Clip -
        # dort soll Ring's Beschreibung hinein.
        mapping = CONFIG['ring'].get('camera_names') or {}
        key = mapping.get(camera_name) or sanitize(camera_name)
        values = self._last_push_values.get(key)
        if values:
            self.frigate.remember(key, values)

        return file_name

    async def start(self) -> None:
        await self._login()
        await self.refresh_metadata()
        self.mqtt.start()
        await self._start_listener()

    async def close(self) -> None:
        self.mqtt.stop()

        if self.listener is not None:
            try:
                await self.listener.stop()
            except Exception as e:
                log.debug(f"Listener-Stop: {e}")
            self.listener = None

        if self.session is not None and not self.session.closed:
            await self.session.close()
            # Gibt dem Loop Zeit, SSL-Transports abzuraeumen.
            await asyncio.sleep(0.25)


async def test() -> None:
    cm = CameraManager()
    await cm.start()

    print("Kameras:", list(cm.get_cameras()))
    for camera in cm.get_cameras():
        print(camera, "->", await cm.save_latest_clip(camera))

    await cm.close()


if __name__ == "__main__":
    asyncio.run(test())
