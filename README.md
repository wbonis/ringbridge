# ringbridge — Ring-Kameras → RTSP → Frigate

## Herkunft und Lizenz — bitte zuerst lesen

ringbridge ist eine **Portierung von
[roger-/blinkbridge](https://github.com/roger-/blinkbridge)** auf Ring.
Gleiche Idee, andere Cloud.

Der Anteil ist erheblich, nicht bloß Inspiration:
`main.py`, `ffmpeg.py`, `utils.py`, `stream_server.py` und `config.py`
stammen aus blinkbridge und sind nur punktuell angepasst — `main.py` etwa
weicht in 24 von 174 Zeilen ab. Neu geschrieben sind `ring.py` (ersetzt
`blink.py`), `mqtt.py` und `frigate.py`.

⚠️ **blinkbridge führt keine Lizenzdatei und deklariert auf GitHub keine
Lizenz.** Damit ist die Weiterverwendung des übernommenen Codes
urheberrechtlich nicht geregelt. Dieses Repository enthält deshalb
bewusst **keine** eigene Lizenzdatei für den abgeleiteten Teil — eine
solche zu vergeben stünde uns nicht zu. Wer den Code weiterverwenden
will, klärt das bitte mit dem Urheber von blinkbridge.

## Warum das Ganze

Frigate braucht einen **durchgehenden** Stream. Ring liefert keinen — und
ein erzwungener Dauerstream hat einen teuren Nebeneffekt: Ring stellt dann
**keine Bewegungsereignisse** mehr zu. Das ist kein Gerücht, sondern steht
so in der Scrypted-Ring-README und im ring-mqtt-Wiki, und wir haben es am
2026-08-30 auf einer laufenden Installation nachgemessen: mit
Dauerstream blieben die Bewegungsereignisse aus, ohne kamen sie
zuverlässig.

ringbridge dreht es um:

- Zwischen den Ereignissen läuft eine **lokal erzeugte Standbildschleife**.
  Frigate sieht eine Kamera, die nie ausfällt.
- Bei Bewegung wird die **fertige Cloud-Aufnahme als MP4 geladen** und in
  den laufenden RTSP-Stream eingespielt.
- Es gibt **keine Live-Session** zu Ring → die Motion-Events bleiben heil.

Preis: Der Clip steht erst bereit, wenn Ring ihn transkodiert hat. Rechne
mit **30–60 s Verzögerung**. Für „was war da?" gut, für Live-Alarm nicht.

## Voraussetzungen

- **Ring Protect**: ohne Abo existieren keine Cloud-Aufnahmen, und dann
  hat ringbridge nichts zu holen.
- **MediaMTX** als RTSP-Server. Die mitgelieferte `compose.yaml` startet
  eine eigene Instanz (Port 8555); ein separater Server ist nicht nötig.

## Verhältnis zum Original

| Datei | Herkunft und Änderung |
|---|---|
| `ring.py` | **neu** — ersetzt `blink.py`, spricht `ring_doorbell`; Push (FCM) + Polling, Ereignisfilter, atomarer Clip-Tausch, Umcodierung je Kamera |
| `mqtt.py` | **neu** — Beschreibung/Klassifikation/Snapshot nach MQTT, HA-Discovery |
| `frigate.py` | **neu** — Beschreibung ins Frigate-Ereignis (standardmäßig aus, Gründe im Modul-Docstring) |
| `main.py` | aus blinkbridge; Imports, `CONFIG['blink']` → `CONFIG['ring']`, `None`-Behandlung wenn kein Clip verfügbar |
| `ffmpeg.py` | aus blinkbridge; codec-unabhängige Stream-Auswahl (Ring mischt H.264 und HEVC), Profilnamen normalisiert, eindeutige Zwischendatei je Kamera, Standbild mit eigener Bildrate und CRF |
| `stream_server.py` | aus blinkbridge; Kameranamen ASCII-gefaltet (`Camera B` → `camera_b`), `-rtsp_transport tcp` beim Publizieren |
| `config.py` | aus blinkbridge; `BLINKBRIDGE_CONFIG` → `RINGBRIDGE_CONFIG` |
| `utils.py` | aus blinkbridge, unverändert |
| `Dockerfile` | `python:3.12-slim` statt `alpine`, `ring_doorbell` statt `blinkpy` |

Die Begründungen zu den einzelnen Abweichungen stehen als Kommentare an
der jeweiligen Codestelle.

## Erster Start

Zugangsdaten in `config/config.json` eintragen, dann **interaktiv**
starten — die 2FA-Abfrage ist ein blockierender stdin-Prompt:

    cd /opt/stacks/ringbridge
    docker compose run --rm ringbridge

Nach erfolgreicher Anmeldung liegt das Token in `config/.ring_token.json`
(Rechte 600). Danach reicht:

    docker compose up -d

Die `hardware_id` wird mitgespeichert. Ohne sie sähe Ring bei jedem Start
ein neues Gerät und verlangte erneut 2FA.

## Einstellungen, die aus der Blink-Praxis stammen

Auf diesem Host hat blinkbridge mit den Vorgabewerten Ärger gemacht. Die
Lehren stecken hier schon in `config/config.json`:

| Wert | Vorgabe | hier | warum |
|---|---|---|---|
| `still_video_duration` | 0.5 | **2.0** | Das Standbild ist im Kern ein I-Frame. Bei 0,5 s wird der zweimal pro Sekunde neu übertragen — 4,8 Mbit/s für ein *stehendes Bild*. |
| `poll_interval` | 1 | **30** | Sekündliches Fragen provoziert API-Timeouts. Clips brauchen ohnehin ~30 s. |
| `max_failures` | 3 | **100** | Im Original schaltet ein einzelner API-Hänger die Kamera **dauerhaft** ab (`main.py`: `stream_servers.pop(camera)`). |
| `restart_delay_seconds` | 60 | **30** | Halbiert die Lücke in Frigate nach einem Fehler. |

Zusätzlich muss MediaMTX auf **TCP** stehen (`MTX_RTSPTRANSPORTS=tcp`).
Per UDP zerreißen die I-Frames hochauflösender Kameras
(`invalid FU-A packet`), und Frigate bekommt dann bereits beschädigte
Bilder — was TCP auf der Leseseite nicht mehr reparieren kann.

## Frigate

    go2rtc:
      streams:
        ring_camera_d: rtsp://192.0.2.10:8555/camera_d

    cameras:
      ring_camera_d:
        ffmpeg:
          inputs:
            - path: rtsp://127.0.0.1:8554/ring_camera_d
              input_args: preset-rtsp-restream
              roles: [detect, record]
        detect:
          fps: 5

Frigate meldet am Übergang Standbild→Clip Bewegung. Das ist erwünscht — es
löst die Erkennung aus. Der Rückschnitt zum neuen Standbild vermutlich
auch; falls das doppelte Ereignisse gibt, `motion` nachziehen.
