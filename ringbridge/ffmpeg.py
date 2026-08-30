import subprocess
import json
from pathlib import Path
from typing import Dict, Tuple, Union
import threading
import sys
import logging
from ringbridge.config import *


log = logging.getLogger(__name__)


# ffprobe-Profilname -> Encoder-Profilname. Alles andere wird
# kleingeschrieben durchgereicht; passt es nicht, entfaellt die Option.
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
    """-profile:v nur setzen, wenn der Name fuer den Encoder brauchbar ist."""
    if not profile:
        return []
    name = _PROFILE_MAP.get(profile.lower(), profile.lower())
    if name not in _PROFILE_OK:
        log.debug(f"Profil {profile!r} unbekannt - -profile:v wird weggelassen")
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

        # ringbridge: Original suchte fest nach codec_name 'aac'/'h264'.
        # Ring mischt die Codecs - 'Camera D' liefert HEVC, die anderen
        # H.264. Auswahl ueber codec_type ist codec-unabhaengig; FrameToVideo
        # nimmt den Encoder ohnehin aus params_video['codec_name'], und
        # ffmpeg loest '-c:v hevc' selbst auf libx265 auf.
        stream_audio = next((s for s in js if s.get('codec_type') == 'audio'), {})
        stream_video = next((s for s in js if s.get('codec_type') == 'video'), {})

        return stream_audio, stream_video

class VideoToLastFrame:
    def __init__(self, input_video: Union[str, Path], output_image: Union[str, Path]):
        time_offset_from_end = 1.0

        ffmpeg_params = [
            'ffmpeg',
            *COMMON_FFMPEG_ARGS,
            '-sseof', str(-time_offset_from_end),
            '-i', input_video,
            '-update', '1',
            '-pix_fmt', 'yuv420p',
            '-vf', 'scale=out_range=pc',  # HACK
            '-q:v', '1',
            output_image
        ]
        
        self.process = subprocess.Popen(ffmpeg_params, stdout=sys.stdout, stderr=subprocess.PIPE)

    def wait(self) -> None:
        out, err = self.process.communicate()
        
        if self.process.returncode != 0:
            raise Exception("ffmpeg failed to extract the last frame: " + err.decode('utf-8'))
        
class FrameToVideo:
    def __init__(self, 
                 image_file_name: Union[str, Path], 
                 params_video: Dict, 
                 params_audio: Dict, 
                 output_duration: float=1, 
                 file_name_output_video: Union[str, Path]="output.mp4"):
        time_base_denominator = params_video['time_base'].split('/')[1] # cut off "1/"
        # ringbridge: Das Standbild braucht nicht die Bildrate des Clips.
        # Es ist ein stehendes Bild - jedes zusaetzliche Frame kostet nur
        # x265-Rechenzeit. 2 s bei 25 fps sind 50 Bilder in 1440p und
        # ~10 s Volllast; bei 5 fps sind es 10 Bilder und ~2,8 s.
        # Waehrend dieser Zeit kommt der Publisher derselben Kamera nicht
        # zum Senden -> "payload is too short" bei MediaMTX.
        # None/0 = Bildrate des Clips uebernehmen (Verhalten des Originals).
        fps_value = CONFIG.get('still_video_fps') or params_video['r_frame_rate']
        
        # Create the ffmpeg parameters list
        ffmpeg_params = [
            'ffmpeg',
            *COMMON_FFMPEG_ARGS,
            '-loop', '1',
            '-i', image_file_name,
            # ringbridge: Tonspur nur, wenn der Clip eine hat. Das Original
            # setzt sie voraus (params_audio['channels']/['sample_rate']) und
            # scheitert sonst mit KeyError bzw. AssertionError - die Kamera
            # faellt dann stumm aus, weil das Standbild 0 Byte gross bleibt.
            *(['-f', 'lavfi',
               '-i', f"anullsrc=channel_layout={params_audio['channels']}"
                     f":sample_rate={params_audio['sample_rate']}"]
              if params_audio else []),
            '-c:v', params_video['codec_name'],
            # ringbridge: Es wird ein STANDBILD encodiert - Lookahead,
            # B-Frames und Bewegungssuche sind hier sinnlos. Ohne preset
            # brauchte x265 fuer ein 2-s-Standbild in 2560x1440 ueber
            # 16 s Volllast; waehrenddessen kam der Publisher derselben
            # Kamera nicht mehr rechtzeitig zum Senden und MediaMTX
            # meldete "payload is too short". Qualitaet ist bei einem
            # stehenden Bild zweitrangig.
            *(['-preset', 'ultrafast']
              if params_video['codec_name'] in ('h264', 'hevc') else []),
            '-pix_fmt', params_video['pix_fmt'],
            '-t', str(output_duration),
            '-vf', f"scale={params_video['width']}:{params_video['height']},fps={fps_value}",
            # ringbridge: CRF statt der Bitrate des Clips. Ein Standbild
            # braucht die Datenrate eines Bewegtbilds nicht - und mit
            # fester Bitrate presst der Encoder sie in immer weniger
            # Bilder, sodass die Datei bei sinkender Bildrate WAECHST.
            # Gemessen (1440p HEVC, 2 s @ 5 fps): mit -b:v 5,7 s / 414 KB,
            # mit -crf 30 4,7 s / 213 KB. Schneller UND kleiner.
            # None/0 = Bitrate des Clips (Verhalten des Originals).
            *(['-crf', str(CONFIG['still_video_crf'])]
              if CONFIG.get('still_video_crf')
              else ['-b:v', params_video['bit_rate']]),
            # ringbridge: ffprobe meldet Profilnamen anders, als die Encoder
            # sie erwarten - gross ("Main") und mit Leerzeichen
            # ("Constrained Baseline"). x265 lehnt "Main" ab
            # ("unknown profile <Main>"), x264 kennt kein
            # "constrained baseline". Deshalb normalisieren; ist der Name
            # unbekannt, lassen wir die Option lieber weg als zu scheitern.
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
        # ringbridge: Ausnahmen im Thread gingen verloren - wait() kehrte
        # normal zurueck, und der Aufrufer hielt ein 0-Byte-Standbild fuer
        # gelungen. Genau diese stille Art hat heute zweimal Zeit gekostet
        # (HEVC-Codec, Profilname). Jetzt wird sie in wait() erneut geworfen.
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
        # ringbridge: Der Name war fest 'last_frame.jpg' - fuer ALLE Kameras
        # derselbe, benutzt aus je einem Thread pro Kamera. Bei mehreren
        # Kameras loeschen die sich gegenseitig die Zwischendatei
        # (FileNotFoundError beim unlink). Ableitung vom Zielvideo macht ihn
        # eindeutig, denn das ist pro Kamera und Zeitpunkt eindeutig.
        still_image_file_name = Path(file_name_still_video).with_suffix('.jpg')
        lfg = VideoToLastFrame(file_name_input_video, still_image_file_name) # run in background
        params_audio, params_video = StreamParameters(file_name_input_video).wait()
        lfg.wait()

        if not params_video:
            raise RuntimeError(
                f"kein Videostream in {file_name_input_video} gefunden")
        if not params_audio:
            log.info(f"{Path(file_name_input_video).name}: keine Tonspur - "
                     f"Standbild wird ohne Ton erzeugt")

        # ringbridge: try/finally - die Zwischendatei wurde nur im
        # Erfolgsfall geloescht. Scheitert das Encodieren, blieb sie liegen
        # (am 2026-08-30 mehrfach beobachtet, je 170 KB, nach den
        # HEVC- und Profilnamen-Fehlschlaegen). missing_ok, weil der
        # Fehler auch vor dem Erzeugen aufgetreten sein kann.
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
    