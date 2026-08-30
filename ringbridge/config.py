from pathlib import Path
import logging
import json
from datetime import datetime, timedelta
from typing import Union
import os


__all__ = ['COMMON_FFMPEG_ARGS', 'CONFIG', 'DELAY_RESTART', 'RTSP_URL', 'PATH_VIDEOS', 'PATH_CONCAT', 'PATH_CONFIG']

COMMON_FFMPEG_ARGS = [
    '-hide_banner',
    '-loglevel', 'error',
    '-y',
]

CONFIG = None
DELAY_RESTART = None
RTSP_URL = None
PATH_VIDEOS = None
PATH_CONCAT = None
PATH_CONFIG = None

def load_config_file(file_name: Union[str, Path]) -> None:
    global CONFIG, DELAY_RESTART, RTSP_URL, PATH_VIDEOS, PATH_CONCAT, PATH_CONFIG

    with open(file_name) as f:
        CONFIG = json.load(f)

    DELAY_RESTART = timedelta(seconds=CONFIG['cameras']['restart_delay_seconds'])
    RTSP_URL = f'rtsp://{CONFIG['rtsp_server']['address']}:{CONFIG['rtsp_server']['port']}'

    PATH_VIDEOS = Path(CONFIG['paths']['videos'])
    PATH_CONCAT = Path(CONFIG['paths']['concat'])
    PATH_CONFIG = Path(CONFIG['paths']['config'])

config_file = os.getenv('RINGBRIDGE_CONFIG', 'config.json')
load_config_file(config_file)
 
