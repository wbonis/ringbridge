import subprocess
import unicodedata
import re
import logging
import sys
from typing import Union
from pathlib import Path
from datetime import datetime
from ringbridge.utils import wait_until_file_open
from ringbridge.config import *
from ringbridge.ffmpeg import StillVideoCreator


log = logging.getLogger(__name__)

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
            '-flush_packets', '0',
            '-c:v', 'copy',
            '-c:a', 'copy',
            # ringbridge: ffmpeg otherwise publishes RTSP over UDP.
            # MediaMTX runs with MTX_RTSPTRANSPORTS=tcp here and rejects
            # that with "461 Unsupported Transport". TCP is the right
            # choice anyway: over UDP the I-frames of high-resolution
            # cameras get torn apart.
            '-rtsp_transport', 'tcp',
            '-f', 'rtsp',
            # '-avoid_negative_ts', '1',
            # '-use_wallclock_as_timestamps', '1',
            '-fps_mode', 'drop',
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

        with open(next_concat, 'w') as f:
            f.write("ffconcat version 1.0\n")
            f.write(f"file '{video_file_name.resolve()}'\n") 

        return next_concat

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
                # Expiring is a normal state, not a failure, and it must not
                # abort add_video(): the still below still has to be enqueued
                # and the old one deleted.
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
                            f"the new clip within the wait ({e}) - continuing, "
                            f"the clip plays once the current file ends")
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

    def start_server(self, file_name_initial_video: Union[str, Path]) -> None:
        log.debug(f"{self.stream_name}: starting server with {file_name_initial_video}")
        self._sweep_old_stills()
        self._make_concat_files()
        self.add_video(file_name_initial_video, still_only=True)
        url = self._run_server()

        log.info(f"{self.stream_name}: stream ready at {url}")

    