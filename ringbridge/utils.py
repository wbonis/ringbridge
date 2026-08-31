import subprocess
import time
import os
from pathlib import Path
from typing import List, Union


def get_pids_by_name(process_name: str) -> List[int]:
    pids = []

    for pid_dir in Path('/proc').iterdir():
        if pid_dir.is_dir() and pid_dir.name.isdigit():
            try:
                with open(pid_dir / 'comm', 'r') as f:
                    comm = f.read().strip()
                    if comm == process_name:
                        pids.append(int(pid_dir.name))
            except FileNotFoundError:
                continue

    return pids

def get_open_files(pid: int) -> List[Path]:
    file_names = []
    fd_dir = Path(f'/proc/{pid}/fd')

    if not fd_dir.is_dir():
        return file_names
    
    for fd in fd_dir.iterdir():
        file_names.append(fd.resolve())
        
    return file_names
 
def is_file_open(process_name: str, file_name: Union[str, Path]) -> bool:
    file_name = Path(file_name).resolve()
    pids = get_pids_by_name(process_name) 

    for pid in pids:
        open_files = get_open_files(pid)

        if file_name in open_files:
            return True
                
    return False

def fd_identities(pid: int) -> set:
    """(st_dev, st_ino) of every file the process has open."""
    ids = set()
    fd_dir = Path(f'/proc/{pid}/fd')

    if not fd_dir.is_dir():
        return ids

    for fd in fd_dir.iterdir():
        try:
            st = os.stat(fd)      # follows the descriptor, unlinked or not
            ids.add((st.st_dev, st.st_ino))
        except OSError:
            continue              # descriptor closed between listing and stat

    return ids

def wait_until_file_open(file_path: Union[str, Path], pid: int, timeout: int=10, poll_interval: int=0.1) -> float:
    """
    Wait until `pid` has THIS file open - identified by inode, not by path.

    Comparing paths was wrong here, and wrong silently. Clips are swapped in
    with os.replace(), which is the whole point: a publisher already reading
    the previous file gets to finish it. But its /proc/<pid>/fd entry then
    points at an unlinked inode, which reads back as "<path> (deleted)" and
    never equals the path again. On a camera delivering a 120 s clip every
    ~130 s the live path was therefore never observed at all - 40 s of
    sampling on 2026-08-31 showed nothing but the deleted inode - so every
    wait expired and the still was never swapped back in.

    Note this deliberately pins the inode at call time. If a newer clip
    replaces the file before the publisher reaches this one, the wait times
    out, and that is the correct answer: this clip will never be played. The
    check now reports that honestly instead of reporting it for the wrong
    reason.
    """
    target = Path(file_path)

    try:
        st = target.stat()
    except OSError as e:
        raise TimeoutError(f"{target} cannot be stat'ed: {e}")

    wanted = (st.st_dev, st.st_ino)
    start_time = time.time()

    while True:
        if wanted in fd_identities(pid):
            return time.time() - start_time

        if time.time() - start_time > timeout:
            raise TimeoutError(
                f"Timeout waiting for process {pid} to open {target}")

        time.sleep(poll_interval)

    return time.time() - start_time

def test() -> None:
    file_path = "videos/patio_latest.mp4"
    process_name = "ffmpeg"

    t = time.time()
    print(is_file_open(process_name, file_path))
    print(f"Waited {time.time() - t} seconds")

if __name__ == "__main__":
    test()