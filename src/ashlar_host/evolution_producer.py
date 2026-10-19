# SPDX-License-Identifier: MIT OR Apache-2.0
"""Owned bounded producer transport; no semantic, SDK or publication authority."""
import os
import selectors
import sys
import signal
import stat
import subprocess
import time
from pathlib import Path
from typing import Mapping, Sequence
from .config import HostError


def snapshot(path: Path, maximum: int) -> bytes:
    """Read one bounded regular-file descriptor; no following final symlinks."""
    if type(maximum) is not int or maximum <= 0:
        raise HostError('evolution-producer-refused')
    fd = None
    primary = None
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise HostError('evolution-producer-refused')
        chunks = []
        remaining = maximum + 1
        while remaining:
            raw = os.read(fd, min(65536, remaining))
            if not raw:
                break
            chunks.append(raw)
            remaining -= len(raw)
        if not remaining:
            raise HostError('evolution-producer-refused')
        return b''.join(chunks)
    except BaseException as exc:
        primary = exc
        if isinstance(exc, Exception):
            raise HostError('evolution-producer-refused') from None
        raise
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except BaseException as cleanup:
                if primary is None or isinstance(primary, Exception) and not isinstance(cleanup, Exception):
                    raise
                try:
                    primary.cleanup_failed = True
                except BaseException:
                    pass


from .process_capture import capture_process

def capture(argv: Sequence[str], *, cwd: Path, environment: Mapping[str, str],
            timeout_seconds: int, maximum_output_bytes: int) -> tuple[bytes, bytes]:
    """Original producer profile: at most60 seconds through the owned transport."""
    if type(timeout_seconds)is not int or not 0<timeout_seconds<=60:
        raise HostError('evolution-producer-refused')
    return capture_process(argv,cwd=cwd,environment=environment,
        timeout_seconds=timeout_seconds,maximum_output_bytes=maximum_output_bytes)
