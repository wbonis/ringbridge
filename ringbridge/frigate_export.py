"""
Frigate-Konfigurationsschnipsel erzeugen.

Die Kameradaten stehen ringbridge ohnehin zur Verfuegung — Name, Pfad und
die echten Stream-Parameter, die fuer das Standbild ausgelesen werden. Sie
von Hand in die Frigate-Config zu uebertragen ist fehleranfaellig, und zwar
nachweislich: am 2026-08-30 standen dort fuer alle vier Ring-Kameras
`detect: 1280x720`, waehrend drei davon 1920x1080 lieferten. Beim Umbenennen
einer Kamera musste der Name an drei Stellen nachgezogen werden, und im
`go2rtc`-Block ging es einmal schief.

Geschrieben wird ein eigenstaendiger Block zum Hineinkopieren — die
Frigate-Config bleibt in der Hand des Benutzers. ringbridge fasst sie nicht
an; das Ziel liegt ohnehin auf einem anderen Host.
"""

import logging
from pathlib import Path

from ringbridge.config import *
from ringbridge.ffmpeg import StreamParameters


log = logging.getLogger(__name__)

DEFAULT_ROLES = ['detect', 'record']
DEFAULT_DETECT_FPS = 5


def _stream_size(clip: Path):
    """Breite/Hoehe aus dem zuletzt geladenen Clip der Kamera."""
    try:
        _, video = StreamParameters(str(clip)).wait()
        w, h = video.get('width'), video.get('height')
        return (int(w), int(h)) if w and h else (None, None)
    except Exception as e:
        log.debug(f"{clip.name}: Aufloesung nicht ermittelbar ({e})")
        return (None, None)


def export(stream_names) -> None:
    """
    `stream_names` sind die bereinigten Namen, also die RTSP-Pfade —
    dieselben, die auch als Frigate-Kameraname taugen.
    """
    cfg = CONFIG.get('frigate_export') or {}
    if not cfg.get('enabled'):
        return

    out = Path(cfg.get('output_path') or (PATH_VIDEOS / 'frigate-cameras.yml'))
    host = cfg.get('rtsp_host') or CONFIG['rtsp_server']['address']
    port = cfg.get('rtsp_port') or CONFIG['rtsp_server']['port']
    roles = cfg.get('roles') or DEFAULT_ROLES
    fps = (cfg.get('detect_defaults') or {}).get('fps', DEFAULT_DETECT_FPS)

    go2rtc, cameras = [], []

    for name in sorted(stream_names):
        clip = PATH_VIDEOS / f"{name}_latest.mp4"
        w, h = _stream_size(clip) if clip.exists() else (None, None)

        go2rtc.append(f"    {name}:\n      - rtsp://{host}:{port}/{name}")

        block = [f"  {name}:",
                 "    ffmpeg:",
                 "      inputs:",
                 f"        - path: rtsp://127.0.0.1:8554/{name}",
                 "          input_args: preset-rtsp-restream",
                 f"          roles: [{', '.join(roles)}]",
                 "    detect:",
                 f"      fps: {fps}"]

        # Nur schreiben, was wir wirklich wissen. Ein geratener Wert ist
        # schlechter als keiner: fehlt width/height, liest Frigate sie
        # selbst aus dem Stream.
        if w and h:
            block += [f"      width: {w}", f"      height: {h}"]
        else:
            block += ["      # width/height weggelassen - Frigate liest sie",
                      "      # selbst aus dem Stream"]

        cameras.append("\n".join(block))

    text = (
        "# Von ringbridge erzeugt - zum Hineinkopieren in die Frigate-Config.\n"
        "# Die Aufloesungen stammen aus dem jeweils zuletzt geladenen Clip,\n"
        "# sind also die tatsaechlich gelieferten Werte.\n"
        "# ringbridge veraendert die Frigate-Config nicht selbst.\n\n"
        "go2rtc:\n  streams:\n" + "\n".join(go2rtc) + "\n\n"
        "cameras:\n" + "\n\n".join(cameras) + "\n")

    try:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text)
        log.info(f"Frigate-Schnipsel geschrieben: {out} ({len(cameras)} Kameras)")
    except Exception as e:
        log.error(f"Frigate-Schnipsel nicht schreibbar ({e})")
