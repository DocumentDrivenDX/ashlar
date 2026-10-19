# SPDX-License-Identifier: MIT OR Apache-2.0
"""Terminate actual scenario descendants while their session leader is owned."""
from __future__ import annotations
import os
import signal
import subprocess
from pathlib import Path
import time
from typing import Callable, Mapping
from types import MappingProxyType
from .evolution_producer import capture
from .lifecycle import finish


def terminate_scenario_processes(process, *, failure_observer: Callable[[Mapping[str, object]], None] | None = None):
    """Freeze the actual group, find nested source sessions, kill and confirm exit.

    Public producer capture uses nested sessions. Killing only the outer group
    would leave those behind after worker cancellation. Inventory contains only
    PID/parent/group numbers, never other processes' commands or environments.
    An optional failure observer receives bounded owned numeric identities and
    fixed phase/error-class metadata after mandatory kill/reap was attempted.
    """
    if type(process) is not subprocess.Popen or process.pid <= 0:
        raise ValueError('Actual owned scenario supervisor process required')
    if failure_observer is not None and not callable(failure_observer):
        raise ValueError('Explicit cleanup failure observer required')
    groups = {process.pid}
    primary = None
    phase = 'freeze-root'
    failure_phase = None
    reaped = False
    remaining = None
    try:
        try: os.killpg(process.pid, signal.SIGSTOP)
        except ProcessLookupError: pass
        phase = 'inventory'
        raw, error = capture(['/bin/ps', '-axo', 'pid=,ppid=,pgid='], cwd=Path('/'),
            environment={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'},
            timeout_seconds=2, maximum_output_bytes=262144)
        if error:
            raise ValueError('Bounded numeric process inventory required')
        rows = {}
        for line in raw.splitlines():
            parts = line.split()
            if len(parts) != 3 or any(not part.isdigit() for part in parts):
                raise ValueError('Exact numeric process inventory required')
            pid, parent, group = map(int, parts)
            if pid in rows: raise ValueError('Distinct process identities required')
            rows[pid] = (parent, group)
        if process.pid not in rows or rows[process.pid][1] != process.pid:
            raise PermissionError('Original session leader inventory required')
        owned = {process.pid}
        while True:
            more = {pid for pid, (parent, _) in rows.items() if parent in owned}
            if more <= owned: break
            owned.update(more)
        for pid in owned:
            group = rows[pid][1]
            if group <= 0 or group not in owned:
                raise PermissionError('Owned descendant session ancestry required')
            groups.add(group)
        phase = 'freeze-descendants'
        for group in groups:
            try: os.killpg(group, signal.SIGSTOP)
            except ProcessLookupError: pass
    except BaseException as error:
        primary = error
        failure_phase = phase
    def kill(group):
        try: os.killpg(group, signal.SIGKILL)
        except ProcessLookupError: pass
    def reap():
        nonlocal reaped
        process.wait(timeout=1)
        reaped = True
    try:
        phase = 'kill-reap'
        finish(primary, [lambda group=group: kill(group) for group in groups] + [reap])
        phase = 'confirm-absence'
        end = time.monotonic()+1
        remaining = set(groups)
        while remaining and time.monotonic() < end:
            for group in tuple(remaining):
                try: os.killpg(group, 0)
                except ProcessLookupError: remaining.remove(group)
            if remaining: time.sleep(.02)
        if remaining: raise RuntimeError('Scenario descendant group exit remains unconfirmed')
    except BaseException as error:
        if failure_observer is not None:
            # Only identities established from this actual owned process and its
            # validated ancestry escape. None means absence was not confirmed.
            record = MappingProxyType({'session_leader_pid': process.pid,
                'process_groups': tuple(sorted(groups)),
                'phase': failure_phase or phase,
                'error_class': type(error).__name__,
                'session_leader_reaped': reaped,
                'remaining_groups': None if remaining is None else tuple(sorted(remaining))})
            # Ordinary diagnostic failures preserve the selected error. The
            # first cancellation wins over an ordinary error; an already
            # selected cancellation retains identity, as for all owned cleanup.
            finish(error, [lambda: failure_observer(record)])
        raise
    return {'session_leader_reaped': True, 'groups_absent': True,
            'process_groups': sorted(groups)}
