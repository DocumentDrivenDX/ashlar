"""Explicit serialized thread bridge; native cancellation remains host-owned.

This candidate is not a native-awaitable driver qualification. A host must supply
thread-affine target/context owners and an independently callable, nonblocking
native cancellation controller before this bridge can dispatch anything.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager
from contextvars import copy_context
from copy import deepcopy
from dataclasses import dataclass
from datetime import timedelta
from threading import Event
from time import monotonic
from typing import Any, Callable
from uuid import uuid4

from umf_access.adoption import TargetIdentity
from umf_access.codec_support import revalidate_model
from umf_access.engine_catalog import EngineRead
from umf_access.native_program import canonical_json
from umf_access.pages import ReadContinuation, StaleContinuation
import hashlib
from .access import _parameter

from umf_access.pages import ReadObligationUnmet, StreamLimitReached

from .publication import ResolutionError

PROFILE = 'ashlar-serialized-native-cancel/0.1'


@dataclass(frozen=True)
class SerializedReadHost:
    """Explicit host execution contract, not inferred connection capabilities.

    open_target and activate run only on the single worker. activate must enforce
    its absolute deadline in the actual native execution owner. cancel runs on the
    event-loop thread and must signal native cancellation without waiting for that
    worker. It must accept an operation not yet started or already completed.
    check_scope is nonblocking and performs no database I/O; it checks the caller
    context on the event loop and the explicitly copied context on the worker.
    Context exits must settle native resources before returning or raising.
    """
    profile: str
    scope_token: str
    identity: TargetIdentity
    default_timeout: timedelta
    check_scope: Callable[[str], None]
    open_target: Callable[[str], AbstractContextManager[Any]]
    activate: Callable[[str, float], AbstractContextManager[None]]
    cancel: Callable[[str], None]

    def __post_init__(self):
        if (self.profile != PROFILE or type(self.identity) is not TargetIdentity or type(self.scope_token) is not str or not self.scope_token
                or type(self.default_timeout) is not timedelta
                or self.default_timeout.total_seconds() <= 0
                or any(not callable(v) for v in (self.check_scope, self.open_target,
                                                 self.activate, self.cancel))):
            raise ValueError('Complete explicit serialized native-cancel host contract required')


class SerializedAsyncTarget:
    """AsyncReadTarget bridge with one worker and no connection construction."""
    def __init__(self, host: SerializedReadHost):
        if type(host) is not SerializedReadHost:
            raise ValueError('Original explicit serialized host required')
        self._host = host
        self._worker = ThreadPoolExecutor(max_workers=1, thread_name_prefix='ashlar-read')
        self._gate = asyncio.Lock()
        self._closed = False

    def identity(self):
        self._host.check_scope(self._host.scope_token)
        return self._host.identity

    def _until(self, timeout):
        budget = self._host.default_timeout if timeout is None else timeout
        if type(budget) is not timedelta:
            raise ValueError('Explicit per-operation timeout required')
        if budget.total_seconds() < 0:
            raise ValueError('Nonnegative operation timeout required')
        if budget.total_seconds() == 0:
            raise TimeoutError('Ashlar async operation deadline already expired')
        return monotonic() + budget.total_seconds()

    async def _acquire(self, until):
        if self._closed:
            raise ResolutionError('Serialized target closed')
        self._host.check_scope(self._host.scope_token)
        remaining = until - monotonic()
        if remaining <= 0:
            raise TimeoutError('Ashlar async queue deadline expired')
        async with asyncio.timeout(remaining):
            await self._gate.acquire()
        if self._closed:
            self._gate.release()
            raise ResolutionError('Serialized target closed')

    async def _settle(self, future, operation, until, cancelled):
        """Never abandon a worker, even under repeated Task.cancel()."""
        wrapped = asyncio.wrap_future(future)
        failure = None
        try:
            remaining = until - monotonic()
            if remaining <= 0:
                raise TimeoutError('Ashlar async operation deadline expired')
            return await asyncio.wait_for(asyncio.shield(wrapped), remaining)
        except (asyncio.CancelledError, TimeoutError) as error:
            failure = error
            # Set by the submitter; queued work refuses before native dispatch.
            cancelled.set()
            cancel_error = None
            try:
                self._host.cancel(operation)
            except BaseException as error:
                cancel_error = error
            while not wrapped.done():
                try:
                    await asyncio.shield(wrapped)
                except asyncio.CancelledError:
                    # Cancellation already won. Still own settlement and cleanup.
                    continue
                except BaseException:
                    break
            worker_error = None
            try:
                wrapped.result()
            except BaseException as error:
                worker_error = error
            raise failure from (worker_error or cancel_error)

    async def _step(self, callback, until):
        self._host.check_scope(self._host.scope_token)
        operation = uuid4().hex
        cancelled = Event()

        def work():
            if cancelled.is_set() or monotonic() >= until:
                raise TimeoutError('Ashlar async queued work expired before dispatch')
            self._host.check_scope(self._host.scope_token)
            with self._host.activate(operation, until):
                remaining = until - monotonic()
                if cancelled.is_set() or remaining <= 0:
                    raise TimeoutError('Ashlar async deadline expired before native dispatch')
                value = callback(timedelta(seconds=remaining))
                self._host.check_scope(self._host.scope_token)
                if monotonic() >= until:
                    raise TimeoutError('Ashlar async deadline expired before release')
                return value

        context = copy_context()
        future = self._worker.submit(context.run, work)
        return await self._settle(future, operation, until, cancelled)

    async def _call(self, method, args, timeout, kwargs):
        until = self._until(timeout)
        entry,parameters=args
        if type(entry) is not EngineRead:
            raise ResolutionError('Original immutable engine catalog required')
        program=entry.artifact()
        parameters=revalidate_model(entry.parameters,parameters)
        args=(entry,parameters)
        kwargs=deepcopy(kwargs)
        if method in ('page','_stream_page'):
            after=kwargs.get('after')
            if after is not None:
                if type(after) is not ReadContinuation or after.owner_cursor is None:
                    raise StaleContinuation('Original immutable owner continuation required')
                request=deepcopy(program['request'])
                request['parameters']={name:_parameter(value) for name,value in parameters.model_dump(mode='python').items()}
                identity=hashlib.sha256(canonical_json([self.identity().digest(),entry.artifact_json,entry.context_json,request['parameters']]).encode()).hexdigest()
                if after.identity_digest!=identity:
                    raise StaleContinuation('Original catalog/target/arguments continuation required')
            page_size=kwargs.get('page_size')
            if page_size is not None and (type(page_size) is not int or not 1<=page_size<=999 or type(program.get('pageSize')) is not int or page_size>program['pageSize']):
                raise ValueError('Positive stream page_size within original authored bound required')
        await self._acquire(until)
        try:
            def call(remaining):
                with self._host.open_target(self._host.scope_token) as target:
                    if target.identity() != self._host.identity:
                        raise ResolutionError('Host target identity changed')
                    call_args=args
                    call_kwargs=dict(kwargs)
                    actual_method=method
                    if method=='_stream_page':
                        actual_method='page'
                        page_size=call_kwargs.pop('page_size')
                        if page_size is not None:
                            recompile=getattr(target,'_stream_page_entry',None)
                            if not callable(recompile):
                                raise ReadObligationUnmet('Original smaller page compiler owner required')
                            call_args=(recompile(args[0],page_size),args[1])
                    remaining = until - monotonic()
                    if remaining <= 0:
                        raise TimeoutError('Ashlar target entry exhausted operation deadline')
                    result=getattr(target, actual_method)(*call_args, timeout=timedelta(seconds=remaining), **call_kwargs)
                    return (call_args[0],result) if method=='_stream_page' else result
            value = await self._step(call, until)
            self._host.check_scope(self._host.scope_token)
            return value
        finally:
            self._gate.release()

    async def read(self, entry, parameters, *, timeout=None):
        return await self._call('read', (entry, parameters), timeout, {})

    async def page(self, entry, parameters, *, after=None, timeout=None):
        return await self._call('page', (entry, parameters), timeout, {'after': after})

    async def stream(self, entry, parameters, *, max_rows, page_size=None, timeout=None):
        if type(max_rows) is not int or max_rows<1:
            raise ValueError('Positive explicit stream row bound required')
        emitted=0
        after=None
        while emitted<max_rows:
            # One complete native page scope, including its closing admission,
            # settles on the worker before ANY buffered row is released. The
            # provider owns original target construction and destruction there.
            entry,page=await self._call('_stream_page',(entry,parameters),timeout,
                                  {'after':after,'page_size':page_size})
            if not page.items and page.next_after is not None:
                raise ReadObligationUnmet('Original page made no continuation progress')
            for row in page.items:
                if self._closed:
                    raise ResolutionError('Serialized target closed before row release')
                self._host.check_scope(self._host.scope_token)
                if emitted==max_rows:
                    raise StreamLimitReached(max_rows,truncated=True)
                emitted+=1
                yield row
            if page.next_after is None:
                return
            if emitted==max_rows:
                raise StreamLimitReached(max_rows,truncated=None)
            after=page.next_after

    async def aclose(self):
        # Wait for the current complete page/read scope. No worker/context is
        # held while a stream consumer owns a previously yielded buffered row.
        await self._gate.acquire()
        try:
            self._closed = True
            self._worker.shutdown(wait=True, cancel_futures=False)
        finally:
            self._gate.release()
