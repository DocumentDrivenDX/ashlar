"""Terminate actual scenario descendants while their session leader is owned."""
import os
import signal
import subprocess
from pathlib import Path
import time
from .evolution_producer import capture
from .lifecycle import finish


def terminate_scenario_processes(process):
    """Freeze the actual group, find nested source sessions, kill and confirm exit.

    Public producer capture uses nested sessions. Killing only the outer group
    would leave those behind after worker cancellation. Inventory contains only
    PID/parent/group numbers, never other processes' commands or environments.
    """
    if type(process) is not subprocess.Popen or process.pid <= 0:
        raise ValueError('Actual owned scenario supervisor process required')
    groups = {process.pid}
    primary = None
    try:
        try: os.killpg(process.pid, signal.SIGSTOP)
        except ProcessLookupError: pass
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
        for group in groups:
            try: os.killpg(group, signal.SIGSTOP)
            except ProcessLookupError: pass
    except BaseException as error:
        primary = error
    def kill(group):
        try: os.killpg(group, signal.SIGKILL)
        except ProcessLookupError: pass
    finish(primary, [lambda group=group: kill(group) for group in groups]
           + [lambda: process.wait(timeout=1)])
    end = time.monotonic()+1
    remaining = set(groups)
    while remaining and time.monotonic() < end:
        for group in tuple(remaining):
            try: os.killpg(group, 0)
            except ProcessLookupError: remaining.remove(group)
        if remaining: time.sleep(.02)
    if remaining: raise RuntimeError('Scenario descendant group exit remains unconfirmed')
    return {'session_leader_reaped': True, 'groups_absent': True,
            'process_groups': sorted(groups)}
