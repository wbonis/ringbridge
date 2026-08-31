# ringbridge — Ring cameras → RTSP → Frigate

## Origin and licence — please read first

ringbridge is a **port of
[roger-/blinkbridge](https://github.com/roger-/blinkbridge)** to Ring.
Same idea, different cloud.

## Why this exists

Frigate needs a **continuous** stream. Ring does not provide one — and
forcing a permanent stream has an expensive side effect: Ring then stops
delivering **motion events** altogether. That is not hearsay; it is stated
in the Scrypted Ring plugin README and in the ring-mqtt wiki, and we
measured it on a running installation on 2026-08-30: with a permanent
stream the motion events stopped, without one they arrived reliably.

ringbridge turns it around:

- Between events, a **locally generated still-image loop** runs. Frigate
  sees a camera that never drops out.
- On motion, the **finished cloud recording is downloaded as MP4** and
  spliced into the running RTSP stream.
- There is **no live session** to Ring, so the motion events stay intact.

The price: the clip is only available once Ring has transcoded it. Expect
**30–60 s of delay**. Good for "what happened?", not for live alerting.

## Requirements

- **Ring Protect**: without a subscription there are no cloud recordings,
  and then ringbridge has nothing to fetch.
- **MediaMTX** as the RTSP server. The bundled `compose.yaml` starts its
  own instance (port 8555); no separate server is needed.

## Relationship to the original

| File | Origin and changes |
|---|---|
| `ring.py` | **new** — replaces `blink.py`, talks to `ring_doorbell`; push (FCM) + polling, event filter, atomic clip swap, per-camera transcoding |
| `mqtt.py` | **new** — description/classification/snapshot to MQTT, HA discovery |
| `frigate.py` | **new** — writes the description into the Frigate event (off by default, reasons in the module docstring) |
| `frigate_export.py` | **new** — generates a Frigate camera config snippet |
| `main.py` | from blinkbridge; imports, `CONFIG['blink']` -> `CONFIG['ring']`, handling for "no clip available" |
| `ffmpeg.py` | from blinkbridge; codec-agnostic stream selection (Ring mixes H.264 and HEVC), profile names normalised, per-camera temp file, still with its own frame rate, CRF and preset, audio made optional, worker-thread exceptions re-raised |
| `stream_server.py` | from blinkbridge; camera names ASCII-folded (`Camera B` -> `camera_b`), `-rtsp_transport tcp` when publishing, orphaned stills swept |
| `config.py` | from blinkbridge; `BLINKBRIDGE_CONFIG` -> `RINGBRIDGE_CONFIG` |
| `utils.py` | from blinkbridge, unchanged |
| `Dockerfile` | `python:3.12-slim` instead of `alpine`, `ring_doorbell` instead of `blinkpy` |

The reasoning behind each individual deviation sits as a comment at the
relevant place in the code.

## First start

Put your credentials into `config/config.json`, then start
**interactively** — the 2FA prompt is a blocking read on stdin:

    cd ringbridge
    docker compose run --rm ringbridge

After a successful login the token is stored in
`config/.ring_token.json` (mode 600). From then on:

    docker compose up -d

The `hardware_id` is stored alongside it. Without it, Ring would see a new
device on every start and ask for 2FA again.

## Settings that came out of practice

Running this against real cameras surfaced a number of defaults that do
not hold up. The lessons are already baked into `config/config.json`:

| Key | Upstream | Here | Why |
|---|---|---|---|
| `poll_interval` | 1 | **5** | This is now only the loop tick, not the API rate — see `idle_poll_seconds`. Polling every second provokes API timeouts. |
| `idle_poll_seconds` | — | **30** | How often the history is actually queried without a push. A push shortcuts this and triggers a check on the next tick. |
| `max_failures` | 3 | **100** | Upstream disables a camera **permanently** after a single API hiccup (`main.py`: `stream_servers.pop(camera)`). |
| `restart_delay_seconds` | 60 | **30** | Halves the gap in Frigate after a failure. |
| `still_video_duration` | 0.5 | **2.0** | The still is essentially one I-frame. At 0.5 s it is retransmitted twice a second. |
| `still_video_fps` | — | **5** | The still does not need the clip's frame rate. 2 s at 25 fps is 50 frames of pure encoder cost for a *static image*. |
| `still_video_crf` | — | **30** | CRF instead of the clip's bitrate: faster and smaller. |
| `still_video_preset` | — | **veryfast** | **Not `ultrafast`** — x264 forces "Constrained Baseline" with it and ignores `-profile:v`, which puts a different `profile-level-id` in the SDP than the clips carry. |
| `event_kinds` | — | **motion, ding** | Ring also lists `on_demand` recordings — those are your own live-view sessions, not events. |
| `max_clip_seconds` | — | **180** | Emergency brake. An `on_demand` recording can run for ten minutes. |

MediaMTX additionally has to be set to **TCP**
(`MTX_RTSPTRANSPORTS=tcp`). Over UDP the I-frames of high-resolution
cameras get torn apart (`invalid FU-A packet`), and Frigate then receives
already-damaged pictures — which TCP on the reading side can no longer
repair.

## The rule behind most of the above

Everything in a `-c copy` concat has to share the same stream parameters.
Clip and still are spliced into one another, so codec, resolution, frame
rate, pixel format, profile and audio layout all have to line up. Most of
the odd-looking details in `ffmpeg.py` exist to keep that true. When it is
violated the failure is rarely obvious: the video geometry still corrects
itself through the in-band SPS, while audio format and profile come only
from the SDP and stay wrong for the life of the stream.

## Frigate

`frigate_export` writes a ready-made snippet (default:
`/working/frigate-cameras.yml`) with the real, measured resolutions.
Copy it into your Frigate config — ringbridge never touches that file
itself.

    go2rtc:
      streams:
        camera_d: rtsp://192.0.2.10:8555/camera_d

    cameras:
      camera_d:
        ffmpeg:
          inputs:
            - path: rtsp://127.0.0.1:8554/camera_d
              input_args: preset-rtsp-restream
              roles: [detect, record]
        detect:
          fps: 5

Frigate reports motion at the still->clip transition. That is intended — it
is what triggers detection. The cut back to the new still probably does
too; if that produces duplicate events, tune `motion`.
