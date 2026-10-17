"""Operator-only fixed-label profiling of installed read-only verification.

Usage: with NativeVerificationTrace(fresh_private_path): run_the_public_cli()
Import this file explicitly into the operator wrapper; no product configuration,
SQL interception, frame locals, or runtime authority is introduced.
"""
import json
import os
from pathlib import Path
import stat
import sys
import threading
import time
from types import FunctionType

MAX_BYTES=65536
MAX_EVENTS=96
MAX_DEPTH=32
LABELS=frozenset(('ledger','ledger-body','verify','detail','history','snapshot',
    'bounded-snapshot','committed','bounded-capture','attempt-query'))


def installed_bindings():
    """Bind exact function code objects from the explicitly selected installation."""
    from ashlar_host.driver import NativeDriver,AttemptExecutor
    from ashlar_host.delta_custody import LocalDeltaTransport
    return {'ledger':NativeDriver._validate_evolution_run_ledger,
        'ledger-body':NativeDriver._validate_evolution_run_ledger_uncached,
        'verify':NativeDriver.verify_evolution_publication,
        'detail':LocalDeltaTransport._detail,'history':LocalDeltaTransport.original_history,
        'snapshot':LocalDeltaTransport._snapshot_uncached,
        'bounded-snapshot':LocalDeltaTransport.bounded_snapshot,
        'committed':LocalDeltaTransport.inspect_committed,
        'bounded-capture':LocalDeltaTransport._bounded_native_rows,
        'attempt-query':AttemptExecutor.query}


class NativeVerificationTrace:
    def __init__(self,path):
        self.path=Path(path);self.fd=None;self.diagnostic_failed=False
        self.stack=[];self.events=[];self.counts={};self.totals={};self.dropped=0
        self.started=None;self.codes={};self.previous=None
    def __enter__(self):
        if threading.current_thread()is not threading.main_thread() or sys.getprofile()is not None:
            raise RuntimeError('An unprofiled main thread is required')
        bindings=installed_bindings()
        if (type(bindings)is not dict or not bindings or not set(bindings)<=LABELS
                or any(type(f)is not FunctionType for f in bindings.values())):
            raise ValueError('Fixed installed function bindings required')
        self.codes={f.__code__:label for label,f in bindings.items()}
        if len(self.codes)!=len(bindings):raise ValueError('Distinct installed functions required')
        parent=self.path.parent.lstat()
        if not stat.S_ISDIR(parent.st_mode)or parent.st_uid!=os.getuid()or parent.st_mode&0o077:
            raise PermissionError('Private owned output directory required')
        self.fd=os.open(self.path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        self.started=time.monotonic()
        try:
            self._write();sys.setprofile(self._profile)
        except BaseException:
            os.close(self.fd);self.fd=None;raise
        return self
    def _write(self):
        now=time.monotonic()
        document={'profile':'ashlar-operator-native-verification-trace/0.1',
            'elapsed_seconds':max(0,now-self.started),'diagnostic_failed':self.diagnostic_failed,
            'counts':self.counts,'completed_seconds':self.totals,'durations':'inclusive-nested-not-additive','events':self.events,
            'dropped_events':self.dropped,'active':[{'label':label,'elapsed_seconds':max(0,now-start)}for _,label,start in self.stack]}
        raw=json.dumps(document,sort_keys=True,separators=(',',':')).encode()+b'\n'
        if len(raw)>MAX_BYTES:raise ValueError('Bounded trace capacity exceeded')
        os.lseek(self.fd,0,os.SEEK_SET)
        offset=0
        while offset<len(raw):
            written=os.write(self.fd,raw[offset:])
            if written<=0:raise OSError('Trace write refused')
            offset+=written
        os.ftruncate(self.fd,len(raw))
    def _profile(self,frame,event,arg):
        if event not in ('call','return')or frame.f_code not in self.codes:return
        try:
            label=self.codes[frame.f_code];now=time.monotonic()
            if event=='call':
                if len(self.stack)>=MAX_DEPTH:raise ValueError('Trace depth exceeded')
                self.stack.append((id(frame),label,now))
                self.counts[label]=min(2**63-1,self.counts.get(label,0)+1)
            else:
                if not self.stack or self.stack[-1][0]!=id(frame):raise ValueError('Trace nesting differs')
                _,_,start=self.stack.pop();self.totals[label]=self.totals.get(label,0)+max(0,now-start)
            if len(self.events)<MAX_EVENTS:self.events.append({'event':event,'label':label,'seconds':max(0,now-self.started)})
            else:self.dropped=min(2**63-1,self.dropped+1)
            self._write()
        except BaseException as error:
            # Ordinary observer failure disables diagnostics; cancellation must
            # propagate so the application's independently owned cleanup runs.
            self.diagnostic_failed=True;sys.setprofile(None)
            if not isinstance(error,Exception):raise
    def __exit__(self,kind,value,traceback):
        sys.setprofile(self.previous)
        cancellation=None
        try:
            self._write()
        except BaseException as error:
            self.diagnostic_failed=True
            if not isinstance(error,Exception):cancellation=error
        finally:
            try:os.close(self.fd)
            except BaseException as error:
                self.diagnostic_failed=True
                if cancellation is None and not isinstance(error,Exception):cancellation=error
            self.fd=None
        if cancellation is not None and (value is None or isinstance(value,Exception)):
            raise cancellation
        return False
