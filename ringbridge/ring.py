"""
Ring integration for ringbridge.

A port of blinkbridge's `blink.py` to the Ring cloud. Serves the same
CameraManager interface that `main.py` expects:

    await start()                              log in + first metadata
    get_cameras()                              camera names
    await save_latest_clip(name, force=False)  fetch latest clip -> path
    await check_for_motion(name)               new clip? -> path or None
    await refresh_metadata()
    await close()

Principle (as in blinkbridge): NO live stream is pulled from the Ring
cloud. Instead, after a motion event the finished cloud recording is
downloaded as MP4. Between events a still-image loop runs locally. That is
precisely why Ring keeps delivering its motion events: there is no
permanent live session.

Requires a Ring Protect subscription, since only then do cloud recordings
exist at all.

Tested against ring_doorbell 0.9.14.
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
from ringbridge.ffmpeg import StreamParameters
from ringbridge.mqtt import MqttPublisher
from ringbridge.frigate import FrigateAnnotator


log = logging.getLogger(__name__)

USER_AGENT = "ringbridge/0.1"
CRED_FILE = ".ring_token.json"

# How many history entries to search for a finished recording.
HISTORY_LIMIT = 10
# Fresh recordings are briefly not retrievable after the event even though
# the history already lists them as "ready" -> retry.
DOWNLOAD_ATTEMPTS = 3
DOWNLOAD_RETRY_DELAY = 5

# Which event kinds are eligible at all. Deliberately NOT "on_demand":
# those are live-view sessions, i.e. your own access.
DEFAULT_EVENT_KINDS = ['motion', 'ding']
# Emergency brake against outliers (Ring serves on_demand clips of 10+
# minutes).
DEFAULT_MAX_CLIP_SECONDS = 180

# Per-camera transcoding (ring.transcode). Intended for Ring cameras that
# deliver HEVC: otherwise clip and still come from different HEVC encoders
# (Ring's and libx265), whose parameter sets differ - and splicing those
# with "copy" produces occasional corrupt frames. Transcoded to H.264 both
# come from the same family and the problem disappears (a 2560x1440 H.264
# camera runs without errors).
# Side effect: the still is then built in 0.3 s instead of 4.7 s.
# Not 'ultrafast': x264 forces "Constrained Baseline" with it and ignores
# -profile:v. The transcoded clip would then carry a different profile from
# everything else - see the comment in ffmpeg.py.
TRANSCODE_PRESET = 'veryfast'

# ffmpeg encoder name -> the codec_name ffprobe reports for the result.
# Used only to compare an existing clip against the transcode spec. An
# encoder that is not listed counts as "cannot verify", and the clip is
# left alone - re-encoding on every start would be worse than the
# mismatch this guards against.
ENCODER_CODEC_NAMES = {
    'libx264': 'h264', 'h264': 'h264',
    'h264_qsv': 'h264', 'h264_vaapi': 'h264', 'h264_nvenc': 'h264',
    'libx265': 'hevc', 'hevc': 'hevc',
    'hevc_qsv': 'hevc', 'hevc_vaapi': 'hevc', 'hevc_nvenc': 'hevc',
}

# Push channel (FCM). After a push, the finished recording is polled for
# on every loop tick for this long - Ring still needs time to transcode
# after the event.
PUSH_WINDOW_SECONDS = 300
# Without a push, the history is only queried this often. That lets the
# loop tick (poll_interval) stay small without loading the Ring API.
DEFAULT_IDLE_POLL_SECONDS = 120
PUSH_CRED_FILE = ".ring_push.json"
# Registering with Google's FCM sporadically fails with
# PHONE_REGISTRATION_ERROR and succeeds on the next attempt. Hence several
# tries; once it lands, the credentials are on disk and later starts do not
# register again at all.
LISTENER_START_ATTEMPTS = 4
LISTENER_RETRY_DELAY = 10

# Periodic still refresh. The still is the last frame of the last motion
# clip, so it is by definition the tail of something that moved - it shows
# whatever triggered the event, and keeps showing it until the next one.
# Refreshing from a cloud snapshot bounds that to one interval.
# 0 = off. Shape and key names are aligned with blinkbridge.
DEFAULT_SNAPSHOT_REFRESH_MINUTES = 0

# async_get_snapshot() asks Ring for a new snapshot and then polls
# `retries` times, `delay` apart, for a timestamp newer than the request.
# The library default of 3x1s is too short here: measured on 2026-08-31,
# one camera returned nothing after 3.5s and a 24 KB picture after 21.6s.
# A camera that just delivered will not deliver again straight away, so
# failures stay normal however long the window is.
DEFAULT_SNAPSHOT_RETRIES = 10
DEFAULT_SNAPSHOT_DELAY_SECONDS = 2


def sanitize(name: str) -> str:
    """Camera name -> filename component (identical to stream_server)."""
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
        # Most recent push values per camera, used for Frigate matching.
        self._last_push_values = {}
        # Time of the last push per camera, and of the last history query.
        self._push_at = {}
        self._last_history_check = defaultdict(float)
        # Snapshot refresh: time of the last ATTEMPT (not the last success)
        # and the bytes last received, per camera.
        self._snapshot_at = defaultdict(float)
        self._last_snapshot = {}

    # ------------------------------------------------------------------ auth

    def _save_token(self, token: dict) -> None:
        """token_updater callback: Ring refreshes tokens while running."""
        try:
            self._cred_path.write_text(json.dumps({
                "token": token,
                "hardware_id": self._hardware_id,
            }))
            self._cred_path.chmod(0o600)
            log.debug("Ring token saved")
        except Exception as e:
            log.error(f"could not save Ring token: {e}")

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
                log.warning(f"stored Ring credentials unreadable ({e}), logging in again")
                token = None

        # Stable hardware_id: otherwise Ring sees a new device on every
        # start and asks for 2FA again.
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
            # Expired or revoked token: discard it so the next start goes
            # through the password path (including 2FA) again.
            log.error(f"Ring authentication failed: {e}")
            if self._cred_path.exists():
                log.info("removing invalid Ring token")
                self._cred_path.unlink()
            raise

        log.info("Successfully authenticated with Ring")

    # ------------------------------------------------------------------ push

    def _on_ring_event(self, event) -> None:
        """
        Callback of the FCM listener. Runs on a foreign thread, so it only
        records a timestamp here and does nothing expensive.
        """
        # Log the raw event: per the library, RingEvent carries none of the
        # description texts ("A cat is sitting on the windowsill") that show
        # up in the Ring app. If something does come along, it lands here.
        log.info(f"push: {event.device_name} kind={event.kind} state={event.state} "
                 f"id={event.id} update={event.is_update}")
        log.debug(f"push raw: {vars(event)}")

        kinds = CONFIG['ring'].get('event_kinds', DEFAULT_EVENT_KINDS)
        if event.kind not in kinds:
            return

        self._push_at[event.device_name] = time.time()

    def _wrap_event_parser(self) -> None:
        """
        Protective wrapper around `RingEventListener._get_ring_event`.

        Ring sends message formats the library does not know. Observed:
        `KeyError: 'id'` in `_get_ring_event`, because `data.event.ding.id`
        is absent. The exception travels all the way into the FCM client
        ("Unexpected exception calling notification callback") and the
        event is lost.

        Here the raw message is logged (the Ring app's description texts
        may be in there) and - more importantly - the device name is
        extracted, so even a message we do not understand still works as a
        trigger.

        This reaches into a private method; on a ring_doorbell update,
        check here first.
        """
        original = self.listener._get_ring_event

        def wrapped(msg_data):
            try:
                return original(msg_data)
            except Exception as e:
                # Not a failure: ring_doorbell does not understand this
                # format (data.event.ding has no "id"), but we do - and
                # these are exactly the messages WITH a description text.
                # Hence DEBUG only; the success path logs a readable INFO
                # line further down.
                log.debug(f"push: not parsed by ring_doorbell "
                          f"({type(e).__name__}: {e}) - handling it ourselves")
                try:
                    log.debug("push raw: " + json.dumps(msg_data, ensure_ascii=False)[:2000])
                except Exception:
                    log.debug(f"push raw (not serialisable): {msg_data}")

                # Use it anyway: set the trigger and hand the rich fields
                # to MQTT (description, classification, snapshot) - those
                # exist only here, not in the history API.
                try:
                    self._handle_rich_push(msg_data)
                except Exception as e:
                    # Now it really is an error: neither the library nor
                    # we could make anything of it.
                    log.warning(f"push: message not usable ({e})")
                    log.warning("push raw: " + str(msg_data)[:1000])
                return None

        self.listener._get_ring_event = wrapped

    def _handle_rich_push(self, msg_data: dict) -> None:
        """
        Extract the fields from a Ring push message.

        Layout (measured 2026-08-30):
          android_config -> title, body   (body = the LLM sentence)
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
            log.info(f"push: {name} [{values.get('detection') or '?'}] (no description)")

        # Frigate camera names may differ from the Ring names. Mapped via
        # ring.camera_names so the MQTT topics line up with the Frigate
        # cameras and Home Assistant can relate the two.
        mapping = CONFIG['ring'].get('camera_names') or {}
        key = mapping.get(name) or sanitize(name)

        self._last_push_values[key] = values
        self.mqtt.publish_event(key, name, values)

    async def _start_listener(self) -> None:
        """
        Subscribe to FCM push. An addition, not a replacement: if push
        fails, history polling carries on as the safety net.
        """
        push_path = PATH_CONFIG / PUSH_CRED_FILE

        credentials = None
        if push_path.exists():
            try:
                credentials = json.loads(push_path.read_text())
            except Exception as e:
                log.warning(f"push credentials unreadable ({e}), fetching new ones")

        def save_credentials(creds):
            try:
                push_path.write_text(json.dumps(creds))
                push_path.chmod(0o600)
            except Exception as e:
                log.error(f"could not save push credentials: {e}")

        for attempt in range(1, LISTENER_START_ATTEMPTS + 1):
            try:
                self.listener = RingEventListener(
                    self.ring, credentials, save_credentials)
                if await self.listener.start(timeout=20):
                    self._wrap_event_parser()
                    self.listener.add_notification_callback(self._on_ring_event)
                    log.info(f"push channel (FCM) active (attempt {attempt})")
                    return
                log.warning(f"push channel: start failed "
                            f"(attempt {attempt}/{LISTENER_START_ATTEMPTS})")
            except Exception as e:
                log.warning(f"push channel: error on attempt "
                            f"{attempt}/{LISTENER_START_ATTEMPTS}: {e}")

            self.listener = None
            if attempt < LISTENER_START_ATTEMPTS:
                await asyncio.sleep(LISTENER_RETRY_DELAY)

        log.warning("push channel unavailable - continuing to poll "
                    "(safety net, higher latency)")

    def _should_check(self, camera_name: str) -> bool:
        """Query the history? After a push yes, otherwise only on the idle tick."""
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
            log.warning(f"{camera_name}: camera no longer in the Ring device list")
        return dev

    def _clip_path(self, camera_name: str) -> Path:
        return PATH_VIDEOS / f"{sanitize(camera_name)}_latest.mp4"

    async def _last_ready_recording_id(self, dev) -> Union[int, None]:
        """
        The most recent usable recording.

        Three filters, each born from a concrete failure:

        1. `recording.status == 'ready'` — the library's
           `async_get_last_recording_id()` blindly takes the last history
           entry.
        2. `kind` in `event_kinds` — Ring also lists **`on_demand`**
           recordings, and those are your own live-view sessions. Splicing
           one in shows Frigate hours-old footage of your own access
           (measured here: an 849 s clip at 231 MB).
        3. `duration <= max_clip_seconds` — emergency brake against
           outliers.
        """
        try:
            history = await dev.async_history(limit=HISTORY_LIMIT)
        except Exception as e:
            log.error(f"{dev.name}: history query failed: {e}")
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
        Fetch a recording.

        The primary path is `async_recording_url()`: it returns a signed
        URL on Ring's CDN, and it is the same path the Scrypted Ring plugin
        uses. The library's direct download
        (`/clients_api/dings/<id>/recording`) is only the fallback — it
        intermittently answers 404 for fresh recordings even though the
        history already lists them as `ready`.
        """
        for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
            if attempt > 1:
                log.debug(f"{dev.name}: recording {recording_id}, attempt {attempt} "
                          f"in {DOWNLOAD_RETRY_DELAY}s")
                await asyncio.sleep(DOWNLOAD_RETRY_DELAY)

            if await self._try_download(dev, recording_id, file_name):
                # Transcoding blocks for several seconds -> run it on a
                # thread so the event loop keeps going.
                await asyncio.to_thread(self._transcode, dev.name, file_name)
                return True

        log.error(f"{dev.name}: recording {recording_id} not retrievable after "
                  f"{DOWNLOAD_ATTEMPTS} attempts")
        return False

    async def _try_download(self, dev, recording_id: int, file_name: Path) -> bool:
        """
        Downloads into a side file and only renames at the end.

        Important: `file_name` is the file the running ffmpeg is reading,
        or is about to read. Writing into it directly hands ffmpeg a
        half-written MP4 - visible in the log as "Invalid NAL unit size" /
        "h264_mp4toannexb filter failed". `os.replace()` is atomic on the
        same filesystem; an ffmpeg that already has the old file open reads
        it to the end under POSIX semantics.
        """
        tmp_name = file_name.with_suffix(file_name.suffix + '.part')

        # 1) signed CDN URL
        try:
            url = await dev.async_recording_url(recording_id)
        except Exception as e:
            log.debug(f"{dev.name}: recording_url failed: {e}")
            url = None

        if url:
            try:
                async with self.session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.read()
                        tmp_name.write_bytes(data)
                        log.debug(f"{dev.name}: {len(data)} B via CDN URL -> {file_name}")
                    else:
                        log.debug(f"{dev.name}: CDN URL answered HTTP {resp.status}")
                        data = None
            except Exception as e:
                log.debug(f"{dev.name}: CDN download failed: {e}")
                data = None

            if data and tmp_name.exists() and tmp_name.stat().st_size > 0:
                os.replace(tmp_name, file_name)
                return True

        # 2) Fallback: the library's direct download
        try:
            await dev.async_recording_download(
                recording_id, filename=str(tmp_name), override=True)
        except Exception as e:
            log.debug(f"{dev.name}: direct download failed: {e}")
            tmp_name.unlink(missing_ok=True)
            return False

        if not tmp_name.exists() or tmp_name.stat().st_size == 0:
            log.debug(f"{dev.name}: recording {recording_id} arrived empty")
            tmp_name.unlink(missing_ok=True)
            return False

        os.replace(tmp_name, file_name)
        return True

    def _clip_matches_spec(self, camera_name: str, file_name: Path) -> bool:
        """
        Does a clip found on disk already have what the transcode spec asks
        for?

        A clip this run downloaded went through _transcode. One left over
        from an earlier run may predate the spec, and that matters more
        than it looks: the seed clip is what the still is built from, and
        the first still is what fixes the stream's SDP. Every later still
        is spliced in with `-c copy`, so a stale seed makes the SDP
        disagree with the content for the whole life of the stream -
        visible as MediaMTX logging `invalid NALU` and `payload is too
        short`, and not self-correcting.

        Unverifiable counts as matching; see ENCODER_CODEC_NAMES.
        """
        spec = (CONFIG['ring'].get('transcode') or {}).get(camera_name)
        if not spec:
            return True

        try:
            _, video = StreamParameters(str(file_name)).wait()
        except Exception as e:
            log.debug(f"{camera_name}: clip parameters unreadable ({e}) - "
                      f"leaving the clip as it is")
            return True

        wanted = ENCODER_CODEC_NAMES.get(spec.get('codec', 'libx264'))
        if wanted and video.get('codec_name') != wanted:
            log.info(f"{camera_name}: existing clip is "
                     f"{video.get('codec_name')}, the spec asks for {wanted}")
            return False

        for key in ('width', 'height'):
            if spec.get(key) and str(video.get(key)) != str(spec[key]):
                log.info(f"{camera_name}: existing clip is "
                         f"{video.get('width')}x{video.get('height')}, the "
                         f"spec asks for {spec.get('width')}x{spec.get('height')}")
                return False

        return True

    def _transcode(self, camera_name: str, file_name: Path) -> None:
        """
        Transcode the clip according to the configured spec.

        Through a side file and os.replace(), so an ffmpeg that is already
        reading never meets a half-written file. If the transcode fails,
        the original clip stays in place - better an HEVC clip than none.
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
            log.error(f"{camera_name}: transcode failed ({e}) - "
                      f"keeping the original clip")
            tmp.unlink(missing_ok=True)
            return

        if tmp.exists() and tmp.stat().st_size > 0:
            os.replace(tmp, file_name)
            log.info(f"{camera_name}: clip transcoded to "
                     f"{spec.get('codec','libx264')}"
                     f"{'/' + str(spec['height']) + 'p' if spec.get('height') else ''} "
                     f"in {time.time()-started:.1f}s")
        else:
            log.error(f"{camera_name}: transcode produced an empty file - "
                      f"keeping the original clip")
            tmp.unlink(missing_ok=True)

    # ------------------------------------------------------- CameraManager-API

    def clip_path(self, camera_name: str) -> Path:
        """The camera's current clip - the reference for stream parameters."""
        return self._clip_path(camera_name)

    def _snapshot_interval(self, camera_name: str) -> float:
        cfg = CONFIG.get('snapshot_refresh') or {}

        # Master switch, checked first. Not a duplicate of
        # default_interval_minutes: that one only covers cameras NOT listed
        # in per_camera, so without this there is no single way to turn the
        # feature off - you would have to zero every per-camera entry and
        # remember to restore them. Absent means enabled, so a config that
        # only sets intervals keeps working.
        if not cfg.get('enabled', True):
            return 0.0

        per_camera = cfg.get('per_camera') or {}
        minutes = per_camera.get(
            camera_name,
            cfg.get('default_interval_minutes', DEFAULT_SNAPSHOT_REFRESH_MINUTES))
        return float(minutes or 0) * 60

    async def fetch_snapshot(self, camera_name: str) -> Union[Path, None]:
        """
        Fetch a fresh snapshot if this camera is due for one.

        Returns the path to a JPEG, or None. The caller turns it into a
        still; this only deals with fetching.

        Two details that are load-bearing rather than cosmetic:

        - The attempt timestamp is written BEFORE the request, not after a
          success. Otherwise a camera that never delivers is retried on
          every loop tick - here that would be every few seconds, forever.
          Not hypothetical: three of four cameras on this account return
          0 bytes, because only the newer models support it.
        - Identical bytes are skipped. Re-encoding the still from a picture
          that did not change costs seconds of CPU for no change on screen.
        """
        interval = self._snapshot_interval(camera_name)
        if not interval:
            return None

        now = time.time()
        if now - self._snapshot_at[camera_name] < interval:
            return None

        # Before the attempt, deliberately.
        self._snapshot_at[camera_name] = now

        dev = self._device(camera_name)
        if dev is None:
            return None

        cfg = CONFIG.get('snapshot_refresh') or {}
        retries = int(cfg.get('retries', DEFAULT_SNAPSHOT_RETRIES))
        delay = int(cfg.get('delay_seconds', DEFAULT_SNAPSHOT_DELAY_SECONDS))

        try:
            data = await dev.async_get_snapshot(retries=retries, delay=delay)
        except Exception as e:
            log.debug(f"{camera_name}: snapshot failed ({e})")
            return None

        if not data:
            # Not a missing capability - the earlier wording claimed that and
            # was wrong. Ring simply produced no snapshot newer than the
            # request inside the window.
            log.debug(f"{camera_name}: no snapshot newer than the request "
                      f"within {retries * delay}s")
            return None

        if self._last_snapshot.get(camera_name) == data:
            log.debug(f"{camera_name}: snapshot unchanged, not rebuilding")
            return None

        self._last_snapshot[camera_name] = data

        path = PATH_VIDEOS / f"{sanitize(camera_name)}_snapshot.jpg"
        try:
            path.write_bytes(data)
        except Exception as e:
            log.error(f"{camera_name}: could not write snapshot ({e})")
            return None

        log.debug(f"{camera_name}: fresh snapshot, {len(data)} B")
        return path

    def note_clip_added(self, camera_name: str) -> None:
        """A real clip wins: restart the snapshot interval."""
        self._snapshot_at[camera_name] = time.time()

    async def refresh_metadata(self) -> None:
        log.debug('refreshing device metadata')
        await self.ring.async_update_data()

    def get_cameras(self):
        return [d.name for d in self.ring.video_devices()]

    def _fallback_clip(self, camera_name: str, reason: str) -> Union[Path, None]:
        """
        Fall back to the most recently downloaded clip.

        Without this the camera is skipped and does not exist in Frigate at
        all - just a warning in the log and no other sign. Two of the
        triggers for that are normal operation, not a fault:

        - The event filter. If the last `HISTORY_LIMIT` events are all
          `on_demand` (i.e. our own live-view sessions) or longer than
          `max_clip_seconds`, nothing is left.
        - A hanging Ring API call at container start.

        With the old clip the camera stays visible and shows the last known
        picture until a recording gets through again. Deliberately not a
        black placeholder: Frigate would read the switch to it as motion
        and generate events on a black background.
        """
        file_name = self._clip_path(camera_name)
        if file_name.exists() and file_name.stat().st_size > 0:
            log.warning(f"{camera_name}: {reason} - keeping the most "
                        f"recently downloaded clip")
            return file_name

        log.warning(f"{camera_name}: {reason}, and no earlier clip is "
                    f"available - camera stays without a stream for now")
        return None

    async def save_latest_clip(self, camera_name: str, force: bool = False) -> Union[Path, None]:
        """Fetch the latest available cloud recording (seed for the stream)."""
        file_name = self._clip_path(camera_name)

        if file_name.exists() and not force:
            # Verify rather than trust: this clip seeds the stream, so its
            # parameters become the SDP. Transcoding locally is cheap
            # compared to a fresh download and needs no cloud session.
            if not self._clip_matches_spec(camera_name, file_name):
                await asyncio.to_thread(self._transcode, camera_name, file_name)
            log.debug(f"{camera_name}: skipping download, {file_name} exists")
            return file_name

        dev = self._device(camera_name)
        if dev is None:
            return self._fallback_clip(camera_name, "camera not in the Ring device list")

        recording_id = await self._last_ready_recording_id(dev)
        if recording_id is None:
            return self._fallback_clip(
                camera_name,
                "no suitable cloud recording found (Ring Protect active? "
                "event filter?)")

        if not await self._download(dev, recording_id, file_name):
            return self._fallback_clip(
                camera_name, f"recording {recording_id} not retrievable")

        # Remember the starting point, or this recording counts as "new".
        self.camera_last_record[camera_name] = recording_id

        return file_name

    async def check_for_motion(self, camera_name: str) -> Union[Path, None]:
        """New cloud recording? Then download it and return the path."""
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

        log.debug(f"{camera_name}: new recording {recording_id}")

        file_name = self._clip_path(camera_name)

        if not await self._download(dev, recording_id, file_name):
            # Do not remember it - retry on the next pass.
            return None

        self.camera_last_record[camera_name] = recording_id
        # Recording is in - close the push window for this camera.
        self._push_at.pop(camera_name, None)

        # Frigate will shortly create an event from the spliced clip -
        # that is where Ring's description should go.
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
