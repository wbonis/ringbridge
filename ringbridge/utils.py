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

def wait_until_file_open(file_path: Union[str, Path], pid: int, timeout: int=10, poll_interval: int=0.1) -> float:
    file_path = Path(file_path).resolve()
    start_time = time.time()

    while True:
        if time.time() - start_time > timeout:
            raise TimeoutError(f"Timeout waiting for process {pid} to open {file_path}")
        
        open_files = get_open_files(pid)
        
        if file_path in open_files:
            break

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