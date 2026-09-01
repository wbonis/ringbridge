import os
import subprocess
import threading
import time
import unicodedata
import re
import logging
import sys
from typing import Union
from pathlib import Path
from datetime import datetime
from ringbridge.utils import wait_until_file_open
from ringbridge.config import *
from ringbridge.ffmpeg import StillVideoCreator, StreamParameters


log = logging.getLogger(__name__)


# The fields a stale SDP actually mis-describes. Video geometry and
# profile/level travel in sprop-parameter-sets / profile-level-id, the audio
# layout in the audio fmtp. Frame rate is deliberately ABSENT: it lives in
# the timestamps, not the SDP, and ffprobe derives it differently for a clip
# and for a still built from that clip (343/12 vs 200/7 for the same rate) -
# comparing it produced a false positive within four minutes on the sibling
# project. Compare only what the SDP carries.
# A frozen publisher is restarted at most this often per camera. The
# detection itself already takes 210+ s, so this is a second fence, not the
# primary pacing - it exists so a pathological loop is bounded and visible
# (every restart logs 'server failed', which the monitor alarms on).
AUTO_RESTART_MIN_INTERVAL = 600

SDP_FIELDS = ('width', 'height', 'profile', 'level',
              'sample_rate', 'channels')


def _sdp_shape(file_name: Union[str, Path]):
    """The SDP-relevant parameters of a file, or None if unreadable."""
    try:
        audio, video = StreamParameters(str(file_name)).wait()
    except Exception as e:
        log.debug(f"shape of {file_name} not readable ({e})")
        return None
    merged = {**{k: video.get(k) for k in ('width', 'height', 'profile', 'level')},
              **{k: audio.get(k) for k in ('sample_rate', 'channels')}}
    return {k: str(v) if v is not None else None for k, v in merged.items()}

class StreamServer:
    def __init__(self, stream_name: str):
        self.stream_name = stream_name
        # ringbridge: also ASCII-fold the name - Ring cameras are called
        # things like "Café 2", and non-ASCII characters in an RTSP path
        # only cause trouble.
        # If a different name is configured for this camera
        # (ring.camera_names), that one wins - and it wins everywhere:
        # RTSP path, MQTT topic, Frigate camera. One name instead of
        # three.
        _mapped = (CONFIG.get('ring', {}).get('camera_names') or {}).get(stream_name)
        _source = _mapped or stream_name

        _n = unicodedata.normalize('NFKD', _source).encode('ascii', 'ignore').decode()
        _n = re.sub(r'[^A-Za-z0-9]+', '_', _n).strip('_').lower()
        self.stream_name_sanitized = _n or 'camera'
        self.current_still_video = None
        self._deferred_still_delete = None
        # True while the stream shows a generated text card instead of
        # camera footage (no usable seed clip). Cleared by the first real
        # clip; see Application.check_for_motion for the two heal paths.
        self.placeholder = False
        # Whether the card was built in the camera's real shape (measured
        # from a clip on disk) or from the built-in fallback canon. Decides
        # whether the first real clip can be spliced in-place or needs a
        # publisher restart to fix the SDP.
        self.placeholder_shape_known = False

    # Set from the first file enqueued - that file is what the publisher
    # announces, so it IS the session's SDP. Reset naturally on a stream
    # restart, since that builds a fresh StreamServer.
    _published_shape = None
    _warned_shape = None

    # Class-level on purpose: a restart builds a fresh StreamServer, so an
    # instance attribute would forget the last restart exactly when the
    # rate limit matters.
    _last_auto_restart = {}

    def _run_server(self) -> str:
        output_url = f"{RTSP_URL}/{self.stream_name_sanitized}"
        input_concat_file = PATH_CONCAT / f"{self.stream_name_sanitized}.concat"

        ffmpeg_args = [
            'ffmpeg',
            *COMMON_FFMPEG_ARGS,
            '-fflags', '+igndts+genpts',
            '-re',
            '-stream_loop', '-1',
            '-f', 'concat',
            '-safe', '0',
            '-i', input_concat_file.resolve(),
            '-c:v', 'copy',
            '-c:a', 'copy',
            # ringbridge: ffmpeg otherwise publishes RTSP over UDP.
            # MediaMTX runs with MTX_RTSPTRANSPORTS=tcp here and rejects
            # that with "461 Unsupported Transport". TCP is the right
            # choice anyway: over UDP the I-frames of high-resolution
            # cameras get torn apart.
            '-rtsp_transport', 'tcp',
            # ringbridge: stay under MediaMTX's 1440-byte RTP limit. ffmpeg
            # defaults to 1472, so MediaMTX logged "RTP packets are too big
            # (1460 > 1440), remuxing them into smaller ones" on every path
            # and then produced "payload is too short" errors - but only
            # while a CLIP was playing, never during the still. A still is
            # small and regular; a 1080p clip's I-frames are neither, so the
            # oversized packets and the remuxing only bite there. Measured
            # 2026-08-31: 121 errors in three minutes on one camera, timed to
            # its clip, and 3435 in five seconds on another. Both settled the
            # moment the still came back.
            '-pkt_size', '1200',
            '-f', 'rtsp',
            # '-avoid_negative_ts', '1',
            # '-use_wallclock_as_timestamps', '1',
            # ringbridge: '-fps_mode drop' removed. With -c copy it is not a
            # no-op: it discards all timestamps and has the muxer regenerate
            # them from ONE nominal frame rate - while this stream alternates
            # a 5 fps still and ~25 fps clips, so one of the two was stamped
            # at 5x the wrong rate on the wire. Survived only because
            # Frigate's preset uses -use_wallclock_as_timestamps 1. The
            # concat demuxer produces continuous monotonic timestamps by
            # itself; nothing here needs replacing them.
            # '-flush_packets 0' removed with it: it buffered the last still
            # packets until clip packets pushed them out - a burst exactly at
            # the transition that matters - and buys nothing on RTSP/TCP.
            output_url
        ]
        
        self.process = subprocess.Popen(ffmpeg_args, stdout=sys.stdout, stderr=sys.stderr)

        return output_url

    def _make_concat_files(self) -> str:
        log.debug(f"{self.stream_name}: making concat file")

        next_concat = PATH_CONCAT / f"{self.stream_name_sanitized}_next.concat"
        concat_file = PATH_CONCAT / f"{self.stream_name_sanitized}.concat"

        with open(concat_file, 'w') as f:
            f.write("ffconcat version 1.0\n")
            f.write(f"file '{next_concat.resolve()}'\n")
            f.write(f"option safe 0\n") # needed to propogate 'safe 0' to next concat file
            f.write(f"file '{next_concat.resolve()}'\n")
            f.write(f"option safe 0\n") # needed to propogate 'safe 0' to next concat file

        return concat_file

    def _enqueue_clip(self, video_file_name: Union[str, Path]) -> Path:
        log.debug(f"{self.stream_name}: enqueueing {video_file_name}")

        video_file_name = Path(video_file_name)
        next_concat = PATH_CONCAT / f"{self.stream_name_sanitized}_next.concat"

        # Atomically. The publisher re-opens this file on every pass of the
        # still, i.e. every ~2 s while idle. A plain open/truncate/write has
        # a window in which the reader sees an empty or half-written ffconcat
        # file; the concat demuxer then errors and ffmpeg exits - a full
        # stream teardown from a microsecond race. The clip files already get
        # this treatment; the concat file is written twenty times as often.
        tmp = next_concat.with_suffix('.concat.tmp')
        with open(tmp, 'w') as f:
            f.write("ffconcat version 1.0\n")
            f.write(f"file '{video_file_name.resolve()}'\n")
        os.replace(tmp, next_concat)

        # Whoever wrote last owns the file. The deferred still swap checks
        # this so it cannot overwrite a newer clip.
        self._queued_clip = video_file_name

        self._reconcile_published_shape(video_file_name)

        return next_concat

    def _reconcile_published_shape(self, file_name: Path) -> None:
        """
        Warn when a file entering the concat no longer matches the shape the
        publisher announced.

        Every file reaches the concat through _enqueue_clip, so checking here
        covers every entry point by construction - seed, clip, still,
        deferred swap and snapshot refresh alike. The SDP is fixed for the
        life of the publisher; content that diverges from it plays, but is
        mis-described to every reader (measured 2026-08-31: Ring switched a
        camera's audio from 48 to 16 kHz between recordings and the first
        mismatched clip produced 4846 depacketization errors).

        Ingest normalisation should make this unreachable. It exists for the
        one path normalisation leaves open: _transcode keeps the ORIGINAL
        clip when ffmpeg fails, deliberately - better a mismatched clip than
        none - and that clip then enters the stream silently. Warn only, no
        automatic restart: a teardown is a heavy answer, and per the sibling
        project's experience an auto-repair loop for a state that should not
        occur has a restart loop as its failure mode.
        """
        shape = _sdp_shape(file_name)
        if shape is None:
            return

        if self._published_shape is None:
            self._published_shape = shape
            log.debug(f"{self.stream_name}: published shape {shape}")
            return

        if shape == self._published_shape:
            self._warned_shape = None
            return

        if shape == self._warned_shape:
            return    # already reported this exact divergence

        diff = ", ".join(
            f"{k} {self._published_shape.get(k)} -> {shape.get(k)}"
            for k in SDP_FIELDS
            if shape.get(k) != self._published_shape.get(k))
        log.warning(f"{self.stream_name}: stream shape changed ({diff}) - "
                    f"content plays but the SDP mis-describes it; restart "
                    f"this stream to republish ({file_name.name})")
        self._warned_shape = shape

    def add_video(self, file_name_input_video: Union[str, Path], still_only: bool=False) -> None:
        if not still_only:
            # enqueue fullclip immediately
            self._enqueue_clip(file_name_input_video) 

        # make a timestamped name for the next still video
        dt = datetime.now()
        next_still_video = PATH_VIDEOS / f"{self.stream_name_sanitized}_still_{dt.strftime('%Y-%m-%d_%H-%M-%S-%f')}.mp4"

        # make still video from input video
        log.debug(f"{self.stream_name}: starting creating next still video {next_still_video}")
        svc = StillVideoCreator(file_name_input_video,
                                output_duration=CONFIG['still_video_duration'],
                                file_name_still_video=next_still_video)
        
        # wait for enqueued video to start
        if not still_only:
            log.debug(f"{self.stream_name}: waiting for new video to start")
            try:
                wait_until_file_open(file_name_input_video, self.process.pid)
            except TimeoutError as e:
                # Expiring is a normal state, not a failure. But it must not
                # fall through to enqueueing the still either: _enqueue_clip()
                # OVERWRITES the concat file, so putting the still in now would
                # replace a clip the publisher has not opened yet, and that
                # clip would never play at all. Measured 2026-08-31: the
                # teardown was gone and so was the motion event. The swap is
                # therefore deferred until the publisher has actually got
                # there.
                #
                # The wait only orders two writes to the concat file - it makes
                # sure ffmpeg has reached the clip before the still is put
                # behind it. But the publisher runs with -re, so it opens the
                # clip only when the demuxer reaches that entry, i.e. once the
                # file currently playing has finished, in real time. The wait
                # is therefore "how much of the current file is left", which is
                # a still-length while idle and up to a whole clip when a
                # second event arrives during playout - well past 10 s with
                # max_clip_seconds at 180. Any timeout below the longest clip
                # will fire eventually on a busy camera, so raising it is not
                # the fix; the bound stays only so add_video() cannot block
                # forever.
                #
                # Left fatal, this closed the stream three times on 2026-08-31,
                # every time on whichever camera was busiest.
                log.warning(f"{self.stream_name}: publisher had not reached "
                            f"the new clip within the wait ({e}) - deferring "
                            f"the still so the clip still gets played")
                svc.wait()
                self._defer_still(Path(file_name_input_video), next_still_video)
                return
            except Exception as e:
                log.warning(f"{self.stream_name}: error waiting for the new "
                            f"clip to open ({e}) - continuing")
            
        # enqueue next still video
        log.debug(f'{self.stream_name}: waiting for still video creation to finish')
        svc.wait()
        self._enqueue_clip(next_still_video)

        # delete old still video
        if self.current_still_video and not still_only:
            log.debug(f'{self.stream_name}: deleting old still video {self.current_still_video}')
            self.current_still_video.unlink()
        
        self.current_still_video = next_still_video
    
    def _defer_still(self, clip: Path, still: Path) -> None:
        """
        Put the still behind the clip once the publisher has really reached it.

        Runs on a thread, because the wait is "how much of the currently
        playing file is left" and that can be a whole clip - far too long to
        hold up the motion loop. Nothing here touches Ring or the network; it
        only rewrites the concat file at the right moment.
        """
        limit = int(CONFIG['ring'].get('max_clip_seconds', 180)) + 30

        def run():
            try:
                wait_until_file_open(clip, self.process.pid, timeout=limit)
            except Exception as e:
                log.warning(f"{self.stream_name}: publisher never reached the "
                            f"clip within {limit}s ({e}) - still not swapped in")

                # Not taking a single segment boundary for 210+ s is
                # impossible in healthy playback of any clip shorter than
                # the bound - measured 2026-08-31: the publisher was asleep
                # inside -re pacing (hrtimer_nanosleep, empty send queue,
                # ESTABLISHED socket), a state no process or socket watchdog
                # can see, and it held for ten minutes until a manual
                # SIGTERM. This warning is therefore a freeze detector, and
                # the cure is a stream restart: terminate the publisher and
                # let the existing watchdog rebuild it (measured: 9 s).
                #
                # Guard 1: only if OUR publisher is still alive. This thread
                # can outlive a watchdog restart, and then self.process is
                # the dead old one - terminating nothing. It can never hit
                # the replacement, because it only ever touches the process
                # object it captured.
                # Guard 2: rate-limited per camera, class-level, so a
                # pathological loop is bounded (and visible: every restart
                # logs 'server failed', which the monitor alarms on).
                if self.process is None or self.process.poll() is not None:
                    log.debug(f"{self.stream_name}: publisher already gone - "
                              f"stale deferred thread, nothing to restart")
                    return

                now = time.time()
                last = StreamServer._last_auto_restart.get(self.stream_name, 0)
                if now - last < AUTO_RESTART_MIN_INTERVAL:
                    log.warning(f"{self.stream_name}: publisher looks frozen "
                                f"again but was auto-restarted "
                                f"{now - last:.0f}s ago - leaving it alone "
                                f"(rate limit {AUTO_RESTART_MIN_INTERVAL}s)")
                    return

                StreamServer._last_auto_restart[self.stream_name] = now
                log.warning(f"{self.stream_name}: publisher frozen - "
                            f"terminating it so the watchdog rebuilds the "
                            f"stream")
                try:
                    self.process.terminate()
                except Exception as e2:
                    log.error(f"{self.stream_name}: could not terminate the "
                              f"frozen publisher ({e2})")
                return

            if self._queued_clip != clip:
                log.debug(f"{self.stream_name}: a newer clip is queued, "
                          f"dropping the deferred still")
                return

            self._enqueue_clip(still)
            old_still, self.current_still_video = self.current_still_video, still
            if old_still and old_still != still:
                try:
                    old_still.unlink()
                except OSError:
                    pass
            log.info(f"{self.stream_name}: clip reached the publisher, "
                     f"still swapped in behind it")

        threading.Thread(target=run, daemon=True).start()

    def is_running(self) -> bool:
        return self.process.poll() is None
    
    def close(self) -> None:
        if self.is_running():
            log.info(f"{self.stream_name}: stopping server")
            self.process.kill()

    def _sweep_old_stills(self) -> None:
        """
        Remove orphaned still files for this camera.

        `add_video()` only deletes the previous still when
        `current_still_video` is set AND `still_only` is False.
        `start_server()` however always calls it with `still_only=True`,
        and on a fresh StreamServer `current_still_video` is None anyway.
        Every container and every stream restart therefore left behind a
        still that was never touched again: on 2026-08-30 that was 103
        files totalling 48 MB in a single day.

        On disk that barely registers; in a tmpfs working directory it
        means ENOSPC in the middle of operation.

        This is a safe point to clean up: the publisher for this camera is
        not running yet, so nothing is reading those files.
        """
        # Include the .jpg intermediates: they are created while building
        # the still and are left behind when the encode fails.
        patterns = (f"{self.stream_name_sanitized}_still_*.mp4",
                    f"{self.stream_name_sanitized}_still_*.jpg")
        removed = 0
        for old_still in (f for pat in patterns for f in PATH_VIDEOS.glob(pat)):
            try:
                old_still.unlink()
                removed += 1
            except OSError as e:
                log.debug(f"{self.stream_name}: {old_still.name} not deletable: {e}")

        if removed:
            log.info(f"{self.stream_name}: removed {removed} orphaned still file(s)")

    def refresh_still_from_image(self, image_file_name: Union[str, Path],
                                 reference_video: Union[str, Path]) -> None:
        """
        Rebuild the still from an arbitrary image, keeping the clip's
        stream parameters.

        The picture content comes from `image_file_name` (a cloud
        snapshot); everything that defines the stream - codec, resolution,
        pixel format, frame rate, profile, audio layout - is read from
        `reference_video`, the camera's own last clip. Anything else would
        put a different profile-level-id or audio format into the SDP than
        the clips carry, which is the failure mode this whole codebase is
        shaped around.

        Deletion of the previous still is deferred by one generation: the
        publisher may still be reading it during the current loop
        iteration, and the concat demuxer reopens the file each time round.
        """
        from ringbridge.ffmpeg import FrameToVideo, StreamParameters

        params_audio, params_video = StreamParameters(str(reference_video)).wait()
        if not params_video:
            log.warning(f"{self.stream_name}: no video parameters from "
                        f"{reference_video} - skipping still refresh")
            return

        dt = datetime.now()
        next_still = PATH_VIDEOS / (f"{self.stream_name_sanitized}_still_"
                                    f"{dt.strftime('%Y-%m-%d_%H-%M-%S-%f')}.mp4")

        FrameToVideo(image_file_name, params_video, params_audio,
                     output_duration=CONFIG['still_video_duration'],
                     file_name_output_video=next_still).wait()

        if not next_still.exists() or next_still.stat().st_size == 0:
            log.warning(f"{self.stream_name}: still refresh produced an "
                        f"empty file - keeping the current still")
            next_still.unlink(missing_ok=True)
            return

        self._enqueue_clip(next_still)

        # One generation behind, see docstring.
        if self._deferred_still_delete:
            self._deferred_still_delete.unlink(missing_ok=True)
        self._deferred_still_delete = self.current_still_video
        self.current_still_video = next_still

        log.info(f"{self.stream_name}: still refreshed from snapshot")

    # Shape used for a placeholder when the camera has never delivered a
    # clip to measure. Video mirrors what the Ring cameras here send
    # (1080p H264 high/4.1); audio is the ingest canon from ring.py, so a
    # later in-place transition at least keeps the audio format. Values
    # are strings because FrameToVideo reads ffprobe output, which is
    # parsed with numbers kept as strings.
    FALLBACK_PARAMS_VIDEO = {
        'codec_name': 'h264', 'width': '1920', 'height': '1080',
        'pix_fmt': 'yuv420p', 'r_frame_rate': '15/1',
        'time_base': '1/90000', 'profile': 'High', 'level': '41',
        'bit_rate': '2000000',
    }
    FALLBACK_PARAMS_AUDIO = {'channels': '1', 'sample_rate': '16000'}

    def start_server_placeholder(self, lines,
                                 reference_video: Union[str, Path, None] = None) -> None:
        """
        Publish a generated text card instead of a seed clip.

        Pattern adopted from blinkbridge (aligned 2026-09-01): a camera
        that cannot be seeded still gets its stream immediately - Frigate
        sees a picture that says what is wrong - and the normal poll loop
        drives it to live footage later. Before this, such a camera was
        skipped and simply did not exist until the next restart.

        `reference_video` is a clip to take the stream shape from (a clip
        whose still creation failed usually still probes fine). Without
        one the fallback canon is used and the first real clip triggers a
        publisher restart instead of an in-place splice.
        """
        from ringbridge.ffmpeg import TextTile, FrameToVideo, StreamParameters

        params_audio, params_video = ({}, {})
        if reference_video is not None:
            try:
                params_audio, params_video = StreamParameters(str(reference_video)).wait()
            except Exception as e:
                log.warning(f"{self.stream_name}: probing {reference_video} "
                            f"for the placeholder shape failed ({e}) - "
                            f"using the fallback shape")

        if params_video:
            self.placeholder_shape_known = True
        else:
            params_video = dict(self.FALLBACK_PARAMS_VIDEO)
            params_audio = dict(self.FALLBACK_PARAMS_AUDIO)
            self.placeholder_shape_known = False

        self._sweep_old_stills()
        self._make_concat_files()

        dt = datetime.now()
        tile = PATH_VIDEOS / (f"{self.stream_name_sanitized}_still_"
                              f"{dt.strftime('%Y-%m-%d_%H-%M-%S-%f')}.jpg")
        still = tile.with_suffix('.mp4')

        TextTile(tile, int(params_video['width']), int(params_video['height']),
                 lines).wait()
        FrameToVideo(tile, params_video, params_audio,
                     output_duration=CONFIG['still_video_duration'],
                     file_name_output_video=still).wait()
        if not still.exists() or still.stat().st_size == 0:
            raise RuntimeError(f"placeholder still {still} came out empty")

        self._enqueue_clip(still)
        self.current_still_video = still
        self.placeholder = True

        url = self._run_server()
        log.info(f"{self.stream_name}: placeholder stream ready at {url} "
                 f"({'camera shape' if self.placeholder_shape_known else 'fallback shape'})")

    def start_server(self, file_name_initial_video: Union[str, Path]) -> None:
        log.debug(f"{self.stream_name}: starting server with {file_name_initial_video}")
        self._sweep_old_stills()
        self._make_concat_files()
        self.add_video(file_name_initial_video, still_only=True)
        url = self._run_server()

        log.info(f"{self.stream_name}: stream ready at {url}")

    