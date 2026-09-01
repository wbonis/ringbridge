import subprocess
import json
from pathlib import Path
from typing import Dict, Tuple, Union
import threading
import sys
import logging
from ringbridge.config import *


log = logging.getLogger(__name__)


# ffprobe profile name -> encoder profile name. Anything else is passed
# through lowercased; if that does not fit, the option is dropped.
_PROFILE_MAP = {
    'constrained baseline': 'baseline',
    'constrained high': 'high',
    'high 10': 'high10',
    'high 4:2:2': 'high422',
    'high 4:4:4 predictive': 'high444',
}
_PROFILE_OK = {'baseline', 'main', 'high', 'high10', 'high422', 'high444',
               'main10', 'mainstillpicture'}


def _profile_args(profile):
    """Only set -profile:v when the name is usable by the encoder."""
    if not profile:
        return []
    name = _PROFILE_MAP.get(profile.lower(), profile.lower())
    if name not in _PROFILE_OK:
        log.debug(f"profile {profile!r} unknown - dropping -profile:v")
        return []
    return ['-profile:v', name]

class StreamParameters:
    def __init__(self, video_file: Union[str, Path]):
        ffprobe_params = [
            'ffprobe',
            '-hide_banner',
            '-loglevel', 'fatal',
            '-show_streams',
            '-print_format', 'json',
            video_file
        ]

        self.process = subprocess.Popen(ffprobe_params, stdout=subprocess.PIPE)

    def wait(self) -> Tuple[Dict, Dict]:
        out, err = self.process.communicate()
        
        if self.process.returncode != 0:
            raise Exception("ffprobe failed to extract parameters: " + err.decode('utf-8'))
        
        # convert json but keep floats and ints as strings
        js = json.loads(out.decode('utf-8'), parse_float=lambda x: x, parse_int=lambda x: x)
        js = js['streams']

        # ringbridge: the original looked for codec_name 'aac'/'h264'
        # specifically. Ring mixes codecs - one camera delivers HEVC, the
        # others H.264. Selecting by codec_type is codec-agnostic;
        # FrameToVideo takes the encoder from params_video['codec_name']
        # anyway, and ffmpeg resolves '-c:v hevc' to libx265 by itself.
        stream_audio = next((s for s in js if s.get('codec_type') == 'audio'), {})
        stream_video = next((s for s in js if s.get('codec_type') == 'video'), {})

        return stream_audio, stream_video

class VideoToLastFrame:
    def __init__(self, input_video: Union[str, Path], output_image: Union[str, Path]):
        # 4 s, not 1 s: -sseof is relative to the CONTAINER end, but Ring
        # clips can have the audio track outlast the video track (measured
        # 2026-09-01: video ends 48.2 s, container 49.7 s). With -sseof -1
        # the seek then lands past the last video packet, ffmpeg encodes
        # zero frames and still EXITS 0 with no output file. Keyframes come
        # every ~2 s, so 4 s covers track skew plus one GOP. The window only
        # sets how many trailing frames are decoded; -update 1 keeps the
        # last one either way.
        time_offset_from_end = 4.0

        ffmpeg_params = [
            'ffmpeg',
            *COMMON_FFMPEG_ARGS,
            '-sseof', str(-time_offset_from_end),
            '-i', input_video,
            '-update', '1',
            # yuvj420p, not yuv420p: the mjpeg encoder wants full-range and
            # refuses limited-range input under strict compliance ("Non
            # full-range YUV is non-standard"). The old pairing - limited
            # pix_fmt against a full-range scale filter - failed on 2 of 267
            # clips depending on their color_range metadata, and one failing
            # clip blocks its camera's seed DETERMINISTICALLY on every
            # restart: measured 2026-09-01, ALL streams stayed down for
            # minutes because one camera's latest clip kept failing here.
            '-pix_fmt', 'yuvj420p',
            '-vf', 'scale=out_range=pc',
            '-q:v', '1',
            output_image
        ]
        
        self.process = subprocess.Popen(ffmpeg_params, stdout=sys.stdout, stderr=subprocess.PIPE)

    def wait(self) -> None:
        out, err = self.process.communicate()
        
        if self.process.returncode != 0:
            raise Exception("ffmpeg failed to extract the last frame: " + err.decode('utf-8'))
        
_TILE_FONT = '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf'
_TILE_FONT_BOLD = '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf'


def _drawtext_escape(text: str) -> str:
    """
    Escape for a drawtext text='...' value (expansion=none is set).

    The quotes do NOT protect ':' - the filtergraph parser strips them
    before the option parser splits on colons, so an unescaped ':' in the
    text (a clock time, say) breaks parsing mid-string. Seen first try.
    ffmpeg's own quotes are sidestepped by turning them typographic.
    """
    return (text.replace('\\', '\\\\')
                .replace(':', '\\:')
                .replace("'", "’"))


class TextTile:
    """
    Render a text card as a JPEG - the picture for placeholder streams.

    Adopted from blinkbridge's placeholder screens (aligned 2026-09-01):
    a camera without a usable seed clip publishes a generated card instead
    of not existing at all. The card carries the camera name plus a line
    saying WHY there is no picture yet - per user request not just
    "Starting...".

    Only the image: turning it into a still video goes through the same
    FrameToVideo as every snapshot, so codec/profile/audio stay consistent
    with the rest of the stream.
    """
    def __init__(self, output_image: Union[str, Path],
                 width: int, height: int, lines):
        # First line = title (bold, larger), the rest smaller below it.
        filters = []
        title_size = max(height // 12, 16)
        line_size = max(height // 22, 12)
        y = 0.32
        for i, line in enumerate(lines):
            bold = i == 0
            filters.append(
                "drawtext=fontfile={font}:text='{text}':expansion=none"
                ":fontcolor={color}:fontsize={size}"
                ":x=(w-text_w)/2:y=h*{y:.3f}".format(
                    font=_TILE_FONT_BOLD if bold else _TILE_FONT,
                    text=_drawtext_escape(str(line)),
                    color='white' if bold else '0xbbbbbb',
                    size=title_size if bold else line_size,
                    y=y))
            y += 0.14 if bold else 0.08

        ffmpeg_params = [
            'ffmpeg',
            *COMMON_FFMPEG_ARGS,
            '-f', 'lavfi',
            '-i', f'color=c=0x2b2b2b:s={width}x{height}',
            '-frames:v', '1',
            '-update', '1',
            '-vf', ','.join(filters),
            # Same reasoning as VideoToLastFrame: the mjpeg encoder wants
            # full-range input.
            '-pix_fmt', 'yuvj420p',
            '-q:v', '2',
            str(output_image)
        ]

        self.output_image = Path(output_image)
        self.process = subprocess.Popen(ffmpeg_params, stdout=sys.stdout,
                                        stderr=subprocess.PIPE)

    def wait(self) -> None:
        out, err = self.process.communicate()
        if self.process.returncode != 0:
            raise Exception("ffmpeg failed to render the text tile: "
                            + err.decode('utf-8'))
        if not self.output_image.exists():
            raise RuntimeError(f"text tile {self.output_image} was not "
                               f"written despite ffmpeg exiting 0")


class FrameToVideo:
    def __init__(self, 
                 image_file_name: Union[str, Path], 
                 params_video: Dict, 
                 params_audio: Dict, 
                 output_duration: float=1, 
                 file_name_output_video: Union[str, Path]="output.mp4"):
        time_base_denominator = params_video['time_base'].split('/')[1] # cut off "1/"
        # ringbridge: the still does not need the clip's frame rate. It
        # is a static image - every extra frame is pure encoder time. 2 s
        # at 25 fps is 50 frames at 1440p and ~10 s of full load; at 5 fps
        # it is 10 frames and ~2.8 s. During that window the publisher for
        # the same camera cannot keep up -> "payload is too short" at
        # MediaMTX.
        # None/0 = inherit the clip's frame rate (original behaviour).
        fps_value = CONFIG.get('still_video_fps') or params_video['r_frame_rate']
        
        # Create the ffmpeg parameters list
        ffmpeg_params = [
            'ffmpeg',
            *COMMON_FFMPEG_ARGS,
            '-loop', '1',
            '-i', image_file_name,
            # ringbridge: audio only when the clip actually has a track.
            # The original assumes one (params_audio['channels'] /
            # ['sample_rate']) and otherwise fails with KeyError or
            # AssertionError - the camera then drops out silently, because
            # the still stays 0 bytes.
            *(['-f', 'lavfi',
               '-i', f"anullsrc=channel_layout={params_audio['channels']}"
                     f":sample_rate={params_audio['sample_rate']}"]
              if params_audio else []),
            '-c:v', params_video['codec_name'],
            # ringbridge: this encodes a STILL IMAGE - lookahead, B-frames
            # and motion search are pointless here, so a fast preset is
            # right. But NOT 'ultrafast': x264 forces "Constrained
            # Baseline" with it and ignores any -profile:v you pass. Since
            # clip and still end up in the same -c copy concat, the
            # profile-level-id in the SDP fmtp then differs between them -
            # the same class of bug as a resolution or audio-format
            # mismatch, just on a parameter nobody looks at.
            # Measured (1080p, 2 s still):
            #   ultrafast 0.5 s  Constrained Baseline  394 KB
            #   veryfast  0.9 s  High                  117 KB
            #   medium    0.7 s  High                  118 KB
            # So ultrafast is also three times the size for a two-tenths
            # of a second lead.
            #
            # Those timings are NOT usable for comparing the presets with
            # each other - the encode is short enough that thread startup
            # and noise dominate, which is why medium appears to beat
            # veryfast. Where the encode runs longer the real gap shows:
            # blinkbridge measures medium 4.76 s against veryfast 2.11 s at
            # 1440p / 2.0 s. The statement above is about profile and file
            # size, not about speed.
            *(['-preset', CONFIG.get('still_video_preset') or 'veryfast']
              if params_video['codec_name'] in ('h264', 'hevc') else []),
            '-pix_fmt', params_video['pix_fmt'],
            '-t', str(output_duration),
            '-vf', f"scale={params_video['width']}:{params_video['height']},fps={fps_value}",
            # ringbridge: CRF instead of the clip's bitrate. A still does
            # not need the data rate of moving video.
            #
            # On the side effect of a fixed bitrate: if you reduce the
            # frame count, the file CAN grow, because the same per-second
            # budget is spread over fewer frames. That only happens when
            # rate control actually binds, though. For a static image the
            # content usually sits far below the budget - and then nothing
            # of the sort occurs. Counter-measurement from the blinkbridge
            # side (1440p, source 2722 kbit/s): 2.0 s -> 285 KB, 0.5 s ->
            # 156 KB, i.e. smaller rather than larger. Here rate control
            # was binding, there it was not.
            # Measured (1440p HEVC, 2 s @ 5 fps): with -b:v 5.7 s / 414 KB,
            # with -crf 30 4.7 s / 213 KB. Faster AND smaller.
            # None/0 = the clip's bitrate (original behaviour).
            *(['-crf', str(CONFIG['still_video_crf'])]
              if CONFIG.get('still_video_crf')
              else ['-b:v', params_video['bit_rate']]),
            # ringbridge: ffprobe reports profile names differently from
            # what the encoders expect - capitalised ("Main") and with
            # spaces ("Constrained Baseline"). x265 rejects "Main"
            # ("unknown profile <Main>"), x264 does not know "constrained
            # baseline". Hence the normalisation; if the name is unknown
            # we drop the option rather than fail.
            *_profile_args(params_video.get('profile')),
            *(['-level:v', str(params_video['level'])]
              if params_video.get('level') not in (None, '', -99) else []),
            '-movflags', 'faststart',
            '-video_track_timescale', time_base_denominator,
            '-fps_mode', 'passthrough',
            *(['-c:a', 'aac',
               '-ar', params_audio['sample_rate'],
               '-ac', params_audio['channels']]
              if params_audio else ['-an']),
            file_name_output_video
        ]    

        # Create the video using ffmpeg
        self.process = subprocess.Popen(ffmpeg_params, stdout=sys.stdout, stderr=subprocess.PIPE)

    def wait(self) -> None:
        out, err = self.process.communicate()

        if self.process.returncode != 0:
            raise Exception(f"ffmpeg failed to create the video: {err.decode('utf-8')}")

class StillVideoCreator:
    def __init__(self, 
                 file_name_input_video: Union[str, Path], 
                 output_duration: float=1, 
                 file_name_still_video: Union[str, Path]="output.mp4"):
        # ringbridge: exceptions inside the thread used to be lost -
        # wait() returned normally and the caller took a 0-byte still for
        # success. Exactly that silent shape cost us time twice (HEVC
        # codec, profile name). They are now re-raised in wait().
        self._error = None
        self.thread = threading.Thread(
            target=self._run_guarded,
            args=(file_name_input_video, output_duration, file_name_still_video))
        self.thread.start()

    def _run_guarded(self, *args) -> None:
        try:
            self._run(*args)
        except Exception as e:
            self._error = e

    def _run(self, 
             file_name_input_video: Union[str, Path], 
             output_duration: float=1, 
             file_name_still_video: Union[str, Path]="output.mp4") -> None:
        # ringbridge: the name used to be a fixed 'last_frame.jpg' - the
        # same one for ALL cameras, used from one thread per camera. With
        # several cameras they delete each other's intermediate file
        # (FileNotFoundError on unlink). Deriving it from the target video
        # makes it unique, since that is unique per camera and timestamp.
        still_image_file_name = Path(file_name_still_video).with_suffix('.jpg')
        lfg = VideoToLastFrame(file_name_input_video, still_image_file_name) # run in background
        params_audio, params_video = StreamParameters(file_name_input_video).wait()
        lfg.wait()
        # ffmpeg can encode ZERO frames and still exit 0 (seen 2026-09-01:
        # -sseof past the end of the video track). Without this check the
        # missing jpg only surfaces as a confusing error from the next
        # stage - or not at all - and the camera's seed hangs the startup.
        if not Path(still_image_file_name).exists():
            raise RuntimeError(
                f"last-frame extraction of {file_name_input_video} produced "
                f"no image (ffmpeg exited 0 but wrote nothing - video track "
                f"shorter than the container?)")

        if not params_video:
            raise RuntimeError(
                f"no video stream found in {file_name_input_video}")
        if not params_audio:
            log.info(f"{Path(file_name_input_video).name}: no audio track - "
                     f"building the still without audio")

        # ringbridge: try/finally - the intermediate file used to be
        # deleted only on success. When the encode failed it was left
        # behind (observed repeatedly on 2026-08-30, 170 KB each, after
        # the HEVC and profile-name failures). missing_ok, because the
        # error can also occur before the file is even created.
        try:
            FrameToVideo(still_image_file_name, params_video, params_audio,
                         output_duration=output_duration,
                         file_name_output_video=file_name_still_video).wait()
        finally:
            still_image_file_name.unlink(missing_ok=True)
        
    def wait(self) -> None:
        self.thread.join()
        if self._error is not None:
            raise self._error
    