import asyncio
import signal
import logging
import os
from datetime import datetime, timedelta
from collections import defaultdict
from rich.console import Console
from rich.logging import RichHandler
from rich.highlighter import NullHighlighter, JSONHighlighter
from ringbridge.stream_server import StreamServer
from ringbridge.ring import CameraManager
from ringbridge.config import *
from ringbridge.frigate_export import export as export_frigate_snippet


# How long a stream has to run without trouble before its failure count is
# cleared. Without this the count only ever grows: it is set to 0 at startup
# and incremented on every restart, including successful ones, so a
# long-lived process eventually hits max_failures and drops a camera that is
# working fine. Observed here: ~1 Ring API timeout per hour would have
# disabled a camera after roughly four days.
HEALTHY_RESET = timedelta(minutes=30)


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

        # ringbridge: without an initial video no StreamServer may be
        # built - the None otherwise travelled all the way into
        # subprocess.Popen (TypeError) and left behind a "running" server
        # with no concat files.
        if file_name_initial_video is None:
            # Pattern from blinkbridge (aligned 2026-09-01): no seed does
            # not mean no stream. Frigate gets a text card saying why
            # there is no picture yet; the poll loop replaces it with the
            # first real recording.
            log.warning(f"{camera_name}: no clip available - starting "
                        f"placeholder stream")
            return self._start_placeholder(camera_name, [
                'kein Clip aus der Ring-Cloud verfügbar',
                'Stream startet mit der nächsten Aufnahme',
                f'seit {datetime.now():%H:%M}'])

        log.info(f"{camera_name}: starting stream server")
        stream_server = StreamServer(camera_name)
        # One camera's seed failing must not take down the other three.
        # Exactly that happened on 2026-09-01: one clip whose video track
        # ended 1.5 s before the container broke the still creation and all
        # four streams stayed down, on every restart, deterministically.
        try:
            stream_server.start_server(file_name_initial_video)
        except Exception as e:
            log.error(f"{camera_name}: stream server failed to start ({e}) "
                      f"- starting placeholder stream")
            try:
                stream_server.close()
            except Exception:
                pass
            # The broken clip usually still probes fine, so the card can
            # be built in the camera's real shape and the next clip can be
            # spliced in without a publisher restart.
            err = str(e).strip().splitlines()[0][:70]
            return self._start_placeholder(camera_name, [
                'letzter Clip defekt - warte auf nächste Aufnahme',
                err,
                f'seit {datetime.now():%H:%M}',
            ], reference_video=file_name_initial_video)
        self.stream_servers[camera_name] = stream_server

        return stream_server

    def _start_placeholder(self, camera_name: str, info_lines,
                           reference_video=None) -> StreamServer:
        ss = StreamServer(camera_name)
        try:
            ss.start_server_placeholder([camera_name, *info_lines],
                                        reference_video)
        except Exception as e:
            log.error(f"{camera_name}: placeholder stream failed too ({e}) "
                      f"- camera skipped until the next restart")
            try:
                ss.close()
            except Exception:
                pass
            return None
        self.stream_servers[camera_name] = ss
        return ss

    async def check_for_motion(self, camera_name: str) -> bool:
        ss = self.stream_servers[camera_name]

        if not ss.is_running():
            return False

        try:
            file_name_new_clip = await self.cam_manager.check_for_motion(camera_name)
        except Exception as e:
            # A Ring cloud error says nothing about the local stream, which
            # is sitting there serving a perfectly good still. The caller's
            # handler closes the stream, so letting this propagate cost a
            # Frigate reconnect and an SDP renegotiation for every API
            # timeout - three times in the first day, always on whichever
            # camera was busiest. Errors raised further down are a different
            # matter: those come from the stream itself and should close it.
            log.warning(f"{camera_name}: Ring query failed ({e}) - "
                        f"stream left running")
            return False

        if not file_name_new_clip:
            await self._maybe_refresh_still(camera_name, ss)
            return False

        if ss.placeholder and not ss.placeholder_shape_known:
            # The card was built in the fallback shape - the SDP does not
            # match this clip, so an in-place splice would feed wrongly
            # announced packets. One publisher restart around the first
            # real clip fixes the SDP for good; Frigate reconnects once.
            log.info(f"{camera_name}: first clip after fallback-shape "
                     f"placeholder - restarting the stream with real "
                     f"parameters{self.cam_manager.event_summary(camera_name)}")
            ss.close()
            ss_new = StreamServer(camera_name)
            ss_new.failure_count = ss.failure_count
            ss_new.datetime_started = datetime.now()
            ss_new.start_server(file_name_new_clip)
            self.stream_servers[camera_name] = ss_new
            self.cam_manager.note_clip_added(camera_name)
            return True

        log.info(f"{ss.stream_name}: motion detected, adding video"
                 f"{self.cam_manager.event_summary(camera_name)}")
        ss.add_video(file_name_new_clip,
                     source_time=self.cam_manager.last_event_time(camera_name))
        if ss.placeholder:
            ss.placeholder = False
            log.info(f"{camera_name}: live footage replaces the placeholder")
        # A real clip wins - restart the snapshot interval, otherwise a
        # refresh due a minute later would replace a genuinely fresh still.
        self.cam_manager.note_clip_added(camera_name)

        return True

    async def _maybe_refresh_still(self, camera_name: str, ss) -> None:
        """
        ringbridge: periodically rebuild the still from a cloud snapshot.

        Only when the camera actually has a running stream and a clip to
        take stream parameters from - the equivalent of gating on a LIVE
        state. Every failure is swallowed: a stale still is always better
        than a broken one.
        """
        try:
            snapshot = await self.cam_manager.fetch_snapshot(camera_name)
            if snapshot is None:
                return

            reference = self.cam_manager.clip_path(camera_name)
            if not reference.exists():
                return

            ss.refresh_still_from_image(snapshot, reference)
        except Exception as e:
            log.warning(f"{camera_name}: still refresh failed: {e}")
        
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

        # ringbridge: write the Frigate snippet once the streams are up -
        # by then the clips exist and the resolutions are known.
        try:
            export_frigate_snippet(
                [ss.stream_name_sanitized for ss in self.stream_servers.values()])
        except Exception as e:
            log.warning(f"Frigate snippet not generated: {e}")

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
                        # No clip retrievable: count the attempt, retry later.
                        ss.failure_count += 1
                        ss.datetime_started = datetime.now()
                        continue
                    ss_new.failure_count = ss.failure_count + 1
                    ss_new.datetime_started = datetime.now()

                elif (ss.failure_count
                      and datetime.now() > ss.datetime_started + HEALTHY_RESET):
                    # max_failures is meant to catch a camera that keeps
                    # failing, not to tally every hiccup over the life of the
                    # process. See HEALTHY_RESET.
                    log.info(f"{camera_name}: running cleanly since "
                             f"{ss.datetime_started:%H:%M}, clearing failure "
                             f"count ({ss.failure_count})")
                    ss.failure_count = 0

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
        format="%(message)s", datefmt="[%X]", handlers=[RichHandler(highlighter=NullHighlighter(),
                              # Without a TTY rich assumes 80 columns and hard-wraps
                              # every message, which makes the event details unreadable.
                              console=Console(width=160),
                              enable_link_path=False)]
    )
    logging.getLogger('ringbridge').setLevel(CONFIG['log_level'])
    logging.getLogger(__name__).setLevel(CONFIG['log_level'])
    
    asyncio.run(main())

