# SPDX-License-Identifier: MIT OR Apache-2.0
"""Bounded recovery/preflight stage evidence, never a native/authority grant."""
from __future__ import annotations
import json,time
from typing import Callable
from contextlib import contextmanager
from .lifecycle import finish

STAGES=frozenset(('runtime-custody','source-admission','source-plan','prior-capture','ordinal-state','native-target','committed-ordinal','source-closing','ack-scope','binding','composition-closing','runtime-closing','publication-apply','publication-observation','publisher-session','publisher-closing','attempt-observation','effect-recovery'))
ROLES=frozenset(('object_current','edge_current','tombstone','whole_source_history','attempts','manifest'))

class MedicalPreflightTiming:
    """Persist begin before work and bounded completion even when work refuses.

    The owning composition supplies exclusive private writes. A prefix shows the
    last entered stage, not a diagnosed cause or permission to skip checks.
    """
    def __init__(self,write: Callable[[str,bytes,int],None]):
        if not callable(write):raise ValueError('Explicit preflight evidence writer required')
        self.write=write;self.records=0

    def emit(self,stage: str,phase: str,*,role: str|None=None,ordinal: int|None=None,duration_ns: int|None=None,outcome: str|None=None) -> None:
        if (type(stage)is not str or stage not in STAGES or type(phase)is not str or phase not in ('begin','end')
                or role is not None and(type(role)is not str or role not in ROLES)
                or ordinal is not None and(type(ordinal)is not int or not 0<=ordinal<32)
                or phase=='begin'and(duration_ns is not None or outcome is not None)
                or phase=='end'and(type(duration_ns)is not int or not 0<=duration_ns<2**63 or type(outcome)is not str or outcome not in ('completed','refused'))
                or self.records>=128):raise ValueError('Closed bounded preflight timing required')
        row={'profile':'ashlar-medical-preflight-stage/0.1','sequence':self.records,'stage':stage,'phase':phase}
        for key,value in (('role',role),('ordinal',ordinal),('duration_ns',duration_ns),('outcome',outcome)):
            if value is not None:row[key]=value
        body=(json.dumps(row,sort_keys=True,separators=(',',':'))+'\n').encode()
        if len(body)>1024:raise ValueError('Bounded preflight timing required')
        self.write('preflight-stage-'+str(self.records).zfill(3)+'.json',body,1024);self.records+=1

    def call(self,stage: str,action: Callable,*,role: str|None=None,ordinal: int|None=None):
        if not callable(action):raise ValueError('Explicit preflight action required')
        errors=[];primary=None;result=None;start=time.monotonic_ns();first_cancellation=None
        try:self.emit(stage,'begin',role=role,ordinal=ordinal)
        except BaseException as error:
            errors.append(error)
            if first_cancellation is None and not isinstance(error,Exception):first_cancellation=error
        try:result=action()
        except BaseException as error:
            primary=error
            if first_cancellation is None and not isinstance(error,Exception):first_cancellation=error
        try:self.emit(stage,'end',role=role,ordinal=ordinal,duration_ns=max(0,time.monotonic_ns()-start),outcome='completed'if primary is None else'refused')
        except BaseException as error:
            errors.append(error)
            if first_cancellation is None and not isinstance(error,Exception):first_cancellation=error
        def fail(error):raise error
        finish(first_cancellation if first_cancellation is not None else primary,[lambda error=error:fail(error)for error in errors])
        return result

    @contextmanager
    def context(self,stage: str,closing: str,manager):
        acquired=False;primary=None;value=None
        def enter():
            nonlocal acquired,value
            value=manager.__enter__();acquired=True;return value
        try:
            self.call(stage,enter)
            try:yield value
            except BaseException as error:primary=error
        except BaseException as error:primary=error
        def close():return manager.__exit__(type(primary)if primary is not None else None,primary,primary.__traceback__ if primary is not None else None)
        finish(primary,([]if not acquired else[lambda:self.call(closing,close)]))
