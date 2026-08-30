import asyncio
import signal
import logging
import os
from datetime import datetime, timedelta
from collections import defaultdict
from rich.logging import RichHandler
from rich.highlighter import NullHighlighter, JSONHighlighter
from ringbridge.stream_server import StreamServer
from ringbridge.ring import CameraManager
from ringbridge.config import *


log = logging.getLogger(__name__)

class Application:
    def __init__(self):
        self.stream_servers = {}
        self.cam_manager = None
        self.running = False

    async def start_stream(self, camera_name: str, redownload: bool=False) -> StreamServer:
        if redownload:
            await self.cam_manager.refresh_metadata()

        log.debug(f"{camera_name}: getting latest clip")
        file_name_initial_video = await self.cam_manager.save_latest_clip(camera_name, force=redownload)

        # ringbridge: Ohne Startvideo darf kein StreamServer gebaut werden -
        # das None wanderte sonst bis in subprocess.Popen (TypeError) und
        # hinterliess einen "laufenden" Server ohne Concat-Dateien.
        if file_name_initial_video is None:
            log.warning(f"{camera_name}: kein Clip verfuegbar, Stream wird nicht gestartet")
            return None

        log.info(f"{camera_name}: starting stream server")
        stream_server = StreamServer(camera_name)
        stream_server.start_server(file_name_initial_video)  
        self.stream_servers[camera_name] = stream_server

        return stream_server

    async def check_for_motion(self, camera_name: str) -> bool:
        ss = self.stream_servers[camera_name]

        if not ss.is_running():
            return False 
        
        file_name_new_clip = await self.cam_manager.check_for_motion(camera_name)

        if not file_name_new_clip:
            return False

        log.info(f"{ss.stream_name}: motion detected, adding video")
        ss.add_video(file_name_new_clip)

        return True
        
    async def start(self) -> None:
        self.running = True
        self.cam_manager = CameraManager()
        await self.cam_manager.start()

        # get enabled cameras
        enabled_cameras = set(CONFIG['cameras']['enabled']) if CONFIG['cameras']['enabled'] else set(self.cam_manager.get_cameras())
        enabled_cameras = enabled_cameras - set(CONFIG['cameras']['disabled'])
        log.info(f"enabled cameras: {enabled_cameras}")      

        # create stream servers for each camera
        for camera in self.cam_manager.get_cameras():
            if camera not in enabled_cameras:
                continue
            
            ss = await self.start_stream(camera)
            if ss is None:
                continue
            ss.failure_count = 0
            ss.datetime_started = datetime.now()

        log.info(f"monitoring cameras for motion")
        while self.running:
            # check for motion on each stream server
            for camera_name in self.stream_servers:
                try:                   
                    await self.check_for_motion(camera_name)
                except Exception as e:
                    log.error(f"{camera_name}: error checking for motion: {e}")
                    self.stream_servers[camera_name].close()

            # check if any stream servers are stopped and restart them
            for camera_name in list(self.stream_servers.keys()):
                ss = self.stream_servers[camera_name]

                if not ss.is_running():
                    # remove stream if too many failures
                    if ss.failure_count >= CONFIG['cameras']['max_failures'] - 1:
                        log.warning(f"{camera_name}: too many failures, disabling")
                        self.stream_servers.pop(camera_name)
                        continue

                    log.warning(f"{camera_name}: server failed {ss.failure_count + 1} time(s)")

                    # do nothing if stream was last started less certain time ago
                    if datetime.now() < ss.datetime_started + DELAY_RESTART:
                        continue

                    # create new stream server
                    ss_new = await self.start_stream(camera_name, redownload=True)
                    if ss_new is None:
                        # Kein Clip abrufbar: Versuch zaehlen, spaeter erneut.
                        ss.failure_count += 1
                        ss.datetime_started = datetime.now()
                        continue
                    ss_new.failure_count = ss.failure_count + 1
                    ss_new.datetime_started = datetime.now()

            await asyncio.sleep(CONFIG['ring']['poll_interval'])

    async def close(self) -> None:
        self.running = False

        if self.cam_manager:
            await self.cam_manager.close()
        
        for ss in self.stream_servers.values():
            ss.close()

async def main() -> None:
    app = Application()
    
    # Create a cancellation event to coordinate shutdown
    shutdown_event = asyncio.Event()

    def handle_exit():
        # Signal the shutdown event when Ctrl+C is received
        shutdown_event.set()

    # Add signal handlers using loop.add_signal_handler
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_exit)

    try:
        # Start the application
        start_task = asyncio.create_task(app.start())
        
        # Wait for shutdown signal
        await shutdown_event.wait()

        log.info("Shutting down...")
        
        # Cancel the start task and wait for it to complete
        start_task.cancel()
        try:
            await start_task
        except asyncio.CancelledError:
            pass

    except Exception as e:
        log.error(f"Unexpected error: {e}")
    
    finally:
        # Ensure app is closed gracefully
        await app.close()

if __name__ == "__main__":
    logging.basicConfig(
        format="%(message)s", datefmt="[%X]", handlers=[RichHandler(highlighter=NullHighlighter())]
    )
    logging.getLogger('ringbridge').setLevel(CONFIG['log_level'])
    logging.getLogger(__name__).setLevel(CONFIG['log_level'])
    
    asyncio.run(main())

