# SPDX-License-Identifier: MIT OR Apache-2.0
"""Bridge lifecycle controls. Fake owners do not qualify native cancellation."""
import asyncio
from contextlib import contextmanager
from datetime import timedelta
from threading import Event, get_ident
from time import monotonic
import unittest

from ashlar.async_access import PROFILE, SerializedAsyncTarget, SerializedReadHost
from umf_access.adoption import TargetIdentity
IDENTITY=TargetIdentity('ashlar','ashlar.databricks','candidate','profile','0'*64,())


class BridgeTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        import json
        from pathlib import Path
        import weft
        from umf_access.engine_catalog import EngineRead
        from umf_access.types import StrictModel
        class Parameters(StrictModel): pass
        class Result(StrictModel): id:str
        original=json.loads((Path(__file__).parent/'fixtures/sdk-engine-related-original.json').read_text())
        request=original['request']
        request['sql']='SELECT r.id AS id FROM sales."order" r'
        request['interfaceVersion']='weft-compile/0.2.2'
        request['target']['backendVersion']='0.1.0-local-spark4-pages-candidate'
        global ENTRY,PARAMS
        ENTRY=EngineRead.build(weft,request,parameters=Parameters,result=Result,method='ashlar-spark4-pages',page_size=3)
        PARAMS=Parameters()
        class NamedParameters(StrictModel): wanted:str
        request['sql']='SELECT r.id AS id FROM sales."order" r WHERE r.id=:wanted'
        request['parameters']={'wanted':{'family':'string','value':'before'}}
        global NAMED_ENTRY,NAMED_PARAMETERS
        NAMED_ENTRY=EngineRead.build(weft,request,parameters=NamedParameters,result=Result,method='ashlar-spark4-pages')
        NAMED_PARAMETERS=NamedParameters

    def host(self, target, *, cancel=None, scope=None):
        self.events = []
        self.worker_threads = []
        @contextmanager
        def opened(token):
            self.worker_threads.append(get_ident())
            self.events.append('open')
            try:
                yield target
            finally:
                self.events.append('closed')
        @contextmanager
        def activated(operation, until):
            self.worker_threads.append(get_ident())
            self.events.append(('activate', until))
            yield
        def check(token):
            if scope is not None:
                scope(token)
        return SerializedReadHost(PROFILE, 'scope', IDENTITY, timedelta(seconds=1),
                                  check, opened, activated, cancel or (lambda _: None))

    async def test_external_cancel_repeated_waits_for_worker_and_cleanup(self):
        entered, release, cancelled = Event(), Event(), Event()
        threads = []
        class Target:
            def identity(self): return IDENTITY
            def read(self, *_args, **_kwargs):
                entered.set()
                release.wait(2)
                return ['secret']
        def cancel(_):
            threads.append(get_ident())
            cancelled.set()
        bridge = SerializedAsyncTarget(self.host(Target(), cancel=cancel))
        task = asyncio.create_task(bridge.read(ENTRY, PARAMS))
        while not entered.is_set(): await asyncio.sleep(0.001)
        task.cancel()
        while not cancelled.is_set(): await asyncio.sleep(0.001)
        task.cancel()
        await asyncio.sleep(0.002)
        self.assertFalse(task.done())
        release.set()
        with self.assertRaises(asyncio.CancelledError): await task
        self.assertEqual(self.events[-1], 'closed')
        self.assertEqual(threads, [get_ident()])
        self.assertTrue(all(thread != get_ident() for thread in self.worker_threads))
        await bridge.aclose()

    async def test_queue_wait_counts_and_does_not_dispatch_expired_call(self):
        entered, release = Event(), Event()
        calls = []
        class Target:
            def identity(self): return IDENTITY
            def read(self, *args, **kwargs):
                calls.append(kwargs['timeout'])
                entered.set()
                release.wait(2)
                return []
        bridge = SerializedAsyncTarget(self.host(Target()))
        first = asyncio.create_task(bridge.read(ENTRY, PARAMS))
        while not entered.is_set(): await asyncio.sleep(0.001)
        with self.assertRaises(TimeoutError):
            await bridge.read(ENTRY, PARAMS, timeout=timedelta(milliseconds=5))
        self.assertEqual(len(calls), 1)
        release.set()
        await first
        await bridge.aclose()

    async def test_stream_no_prefetch_same_worker_close_and_context_change(self):
        from umf_access.pages import ReadPage
        seen=[]
        class Target:
            def identity(self): return IDENTITY
            def page(self,*_args,**kwargs):
                seen.append(('page',kwargs['after'],get_ident()))
                return ReadPage((0,1),None)
        bridge=SerializedAsyncTarget(self.host(Target()))
        stream=bridge.stream(ENTRY,PARAMS,max_rows=3)
        self.assertEqual(await anext(stream),0)
        self.assertEqual(self.events[-1],'closed')
        await asyncio.sleep(.003)
        self.assertEqual(len(seen),1)
        await stream.aclose()
        self.assertEqual(len(seen),1)
        await bridge.aclose()

    async def test_stream_denied_page_closing_withholds_all_rows(self):
        from dataclasses import replace
        from umf_access.pages import ReadPage
        class Target:
            def identity(self): return IDENTITY
            def page(self,*_args,**_kwargs): return ReadPage(('secret',),None)
        @contextmanager
        def denied(_):
            yield Target()
            raise ValueError('stream closing denied')
        bridge=SerializedAsyncTarget(replace(self.host(Target()),open_target=denied))
        stream=bridge.stream(ENTRY,PARAMS,max_rows=3)
        with self.assertRaisesRegex(ValueError,'stream closing denied'):
            await anext(stream)
        await stream.aclose()
        await bridge.aclose()

    async def test_scope_change_refuses_before_native_and_denied_close_withholds(self):
        class Target:
            def identity(self): return IDENTITY
            def read(self, *_args, **_kwargs): raise AssertionError('must not dispatch')
        def refuse(_): raise ValueError('caller changed')
        bridge = SerializedAsyncTarget(self.host(Target(), scope=refuse))
        with self.assertRaisesRegex(ValueError, 'caller changed'):
            await bridge.read(ENTRY, PARAMS)
        self.assertNotIn('open', self.events)
        await bridge.aclose()
        @contextmanager
        def denied(_token):
            class Completed:
                def identity(self): return IDENTITY
                def read(self, *_args, **_kwargs): return ['secret']
            yield Completed()
            raise ValueError('closing admission denied')
        host = self.host(Target())
        from dataclasses import replace
        bridge = SerializedAsyncTarget(replace(host, open_target=denied))
        with self.assertRaisesRegex(ValueError, 'closing admission denied'):
            await bridge.read(ENTRY, PARAMS)
        await bridge.aclose()

    async def test_event_loop_ticks_and_worker_deadline_remaining(self):
        entered, release = Event(), Event()
        observed = []
        class Target:
            def identity(self): return IDENTITY
            def read(self, *_args, **kwargs):
                observed.append(kwargs['timeout'].total_seconds())
                entered.set()
                release.wait(2)
                return []
        bridge = SerializedAsyncTarget(self.host(Target()))
        task = asyncio.create_task(bridge.read(ENTRY, PARAMS, timeout=timedelta(seconds=.5)))
        ticks = 0
        while not entered.is_set(): await asyncio.sleep(.001)
        for _ in range(3):
            await asyncio.sleep(.001)
            ticks += 1
        self.assertFalse(task.done())
        release.set()
        await task
        self.assertEqual(ticks, 3)
        self.assertGreater(observed[0], 0)
        self.assertLess(observed[0], .5)
        await bridge.aclose()

    async def test_missing_contract_refuses(self):
        with self.assertRaises(ValueError): SerializedAsyncTarget(object())
        with self.assertRaises(ValueError):
            SerializedReadHost('unknown', 'scope', 'id', timedelta(seconds=1), None, None, None, None)

    async def test_cancel_before_delivery_never_releases_completed_rows(self):
        loop = asyncio.get_running_loop()
        current = {}
        class Target:
            def identity(self): return IDENTITY
            def read(self, *_args, **_kwargs):
                loop.call_soon_threadsafe(current['task'].cancel)
                return ['must remain withheld']
        bridge = SerializedAsyncTarget(self.host(Target()))
        task = asyncio.create_task(bridge.read(ENTRY, PARAMS))
        current['task'] = task
        with self.assertRaises(asyncio.CancelledError): await task
        self.assertEqual(self.events[-1], 'closed')
        await bridge.aclose()

    async def test_timeout_requests_external_cancel_then_waits_for_cleanup(self):
        release = Event()
        cancelled = []
        class Target:
            def identity(self): return IDENTITY
            def read(self, *_args, **_kwargs):
                release.wait(2)
                return ['withheld']
        def cancel(operation):
            cancelled.append(operation)
            release.set()
        bridge = SerializedAsyncTarget(self.host(Target(), cancel=cancel))
        with self.assertRaises(TimeoutError):
            await bridge.read(ENTRY, PARAMS, timeout=timedelta(milliseconds=8))
        self.assertEqual(len(cancelled), 1)
        self.assertEqual(self.events[-1], 'closed')
        await bridge.aclose()

    async def test_contextvars_caller_and_worker_scope_do_not_borrow_actor(self):
        from contextvars import ContextVar
        actor = ContextVar('bridge_actor', default='none')
        actor.set('alice')
        calls = []
        class Target:
            def identity(self): return IDENTITY
            def read(self, *_args, **_kwargs):
                calls.append(actor.get())
                return []
        def check(_):
            if actor.get() != 'alice': raise ValueError('foreign actor')
        bridge = SerializedAsyncTarget(self.host(Target(), scope=check))
        await bridge.read(ENTRY, PARAMS)
        actor.set('mallory')
        with self.assertRaisesRegex(ValueError, 'foreign actor'):
            await bridge.read(ENTRY, PARAMS)
        self.assertEqual(calls, ['alice'])
        await bridge.aclose()

    async def test_target_entry_time_is_deducted_from_native_method_budget(self):
        from dataclasses import replace
        from time import sleep
        observed = []
        class Target:
            def identity(self): return IDENTITY
            def read(self, *_args, **kwargs):
                observed.append(kwargs['timeout'].total_seconds())
                return []
        @contextmanager
        def opened(_):
            sleep(.008)
            yield Target()
        host = replace(self.host(Target()), open_target=opened)
        bridge = SerializedAsyncTarget(host)
        await bridge.read(ENTRY, PARAMS, timeout=timedelta(milliseconds=40))
        self.assertLess(observed[0], .034)
        await bridge.aclose()

    async def test_cancel_during_gate_grant_does_not_leak_lock(self):
        class Target:
            def identity(self): return IDENTITY
            def read(self, *_args, **_kwargs): return []
        bridge = SerializedAsyncTarget(self.host(Target()))
        await bridge._gate.acquire()
        task = asyncio.create_task(bridge.read(ENTRY, PARAMS))
        await asyncio.sleep(0)
        bridge._gate.release()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError): await task
        self.assertEqual(await bridge.read(ENTRY, PARAMS), [])
        await bridge.aclose()

    async def test_invalid_inputs_and_stale_continuation_refuse_before_open(self):
        from umf_access.pages import ReadContinuation
        class Target:
            def identity(self): return IDENTITY
        bridge=SerializedAsyncTarget(self.host(Target()))
        bad=NAMED_PARAMETERS.model_construct(wanted=3)
        with self.assertRaises(Exception): await bridge.read(NAMED_ENTRY,bad)
        for size in (True,0,4,1000):
            stream=bridge.stream(ENTRY,PARAMS,max_rows=3,page_size=size)
            with self.assertRaises(ValueError): await anext(stream)
            await stream.aclose()
        with self.assertRaises(Exception):
            await bridge.page(ENTRY,PARAMS,after=ReadContinuation(('x',),'foreign',b'opaque'))
        self.assertNotIn('open',self.events)
        await bridge.aclose()

    async def test_parameters_are_snapshotted_before_queue(self):
        captured=[]
        class Target:
            def identity(self): return IDENTITY
            def read(self,_entry,parameters,**_kwargs):
                captured.append(parameters.wanted)
                return []
        bridge=SerializedAsyncTarget(self.host(Target()))
        parameters=NAMED_PARAMETERS(wanted='before')
        await bridge._gate.acquire()
        task=asyncio.create_task(bridge.read(NAMED_ENTRY,parameters))
        await asyncio.sleep(0)
        object.__setattr__(parameters,'wanted','after')
        bridge._gate.release()
        await task
        self.assertEqual(captured,['before'])
        await bridge.aclose()

    async def test_scope_checked_before_each_buffered_stream_row(self):
        from contextvars import ContextVar
        from umf_access.pages import ReadPage
        actor=ContextVar('stream_actor',default='alice')
        calls=[]
        class Target:
            def identity(self): return IDENTITY
            def page(self,*_args,**_kwargs):
                calls.append(1)
                return ReadPage((1,2),None)
        def check(_):
            if actor.get()!='alice': raise ValueError('caller changed')
        bridge=SerializedAsyncTarget(self.host(Target(),scope=check))
        stream=bridge.stream(ENTRY,PARAMS,max_rows=3)
        self.assertEqual(await anext(stream),1)
        actor.set('mallory')
        with self.assertRaisesRegex(ValueError,'caller changed'): await anext(stream)
        self.assertEqual(len(calls),1)
        await stream.aclose()
        await bridge.aclose()

    async def test_real_smaller_original_page_entry_retained_across_continuations(self):
        import hashlib
        from types import SimpleNamespace
        import weft
        from ashlar.access import AshlarTarget
        from umf_access.native_program import canonical_json
        from umf_access.pages import ReadPage,ReadContinuation
        calls=[]
        owner=SimpleNamespace(compiler=weft,compiler_method='ashlar-spark4-pages')
        class Target:
            def identity(self): return IDENTITY
            def _stream_page_entry(self,entry,size):
                return AshlarTarget._stream_page_entry(owner,entry,size)
            def page(self,entry,parameters,*,after,timeout):
                program=entry.artifact()
                self_test.assertEqual(program['pageSize'],1)
                request=program['request']
                after_json='null' if after is None else canonical_json(list(after.values))
                # Actual original R22 page continuation recompile, not mocked
                # compiler metadata. Only native row fetching is a fake owner.
                checked=__import__('json').loads(weft.check_engine_page_json(entry.originals(),canonical_json(request),after_json))
                self_test.assertEqual(checked['status'],'compiled')
                calls.append(entry.artifact_json)
                if after is None:
                    identity=hashlib.sha256(canonical_json([IDENTITY.digest(),entry.artifact_json,entry.context_json,request['parameters']]).encode()).hexdigest()
                    return ReadPage(('first',),ReadContinuation(('a',),identity,b'opaque-native'))
                return ReadPage(('second',),None)
        self_test=self
        bridge=SerializedAsyncTarget(self.host(Target()))
        rows=[row async for row in bridge.stream(ENTRY,PARAMS,max_rows=3,page_size=1)]
        self.assertEqual(rows,['first','second'])
        self.assertEqual(len(calls),2)
        self.assertEqual(calls[0],calls[1])
        self.assertNotEqual(calls[0],ENTRY.artifact_json)
        self.assertEqual(ENTRY.artifact()['pageSize'],3)
        with self.assertRaises(Exception):
            await bridge.page(ENTRY,PARAMS,after=ReadContinuation(('a',),'foreign',b'opaque-native'))
        await bridge.aclose()

    async def test_zero_timeout_is_expired_without_host_open(self):
        class Target:
            def identity(self): return IDENTITY
        bridge=SerializedAsyncTarget(self.host(Target()))
        with self.assertRaises(TimeoutError):
            await bridge.read(ENTRY,PARAMS,timeout=timedelta(0))
        self.assertNotIn('open',self.events)
        await bridge.aclose()

    async def test_negative_and_foreign_timeout_refuse_without_host_open(self):
        class Target:
            def identity(self): return IDENTITY
        bridge=SerializedAsyncTarget(self.host(Target()))
        for timeout in (timedelta(microseconds=-1),True,1):
            with self.assertRaises(ValueError): await bridge.read(ENTRY,PARAMS,timeout=timeout)
        self.assertNotIn('open',self.events)
        await bridge.aclose()

    async def test_closed_target_withholds_remaining_buffered_stream_rows(self):
        from umf_access.pages import ReadPage
        calls=[]
        class Target:
            def identity(self): return IDENTITY
            def page(self,*_args,**_kwargs):
                calls.append(1)
                return ReadPage((1,2),None)
        bridge=SerializedAsyncTarget(self.host(Target()))
        stream=bridge.stream(ENTRY,PARAMS,max_rows=3)
        self.assertEqual(await anext(stream),1)
        await bridge.aclose()
        with self.assertRaisesRegex(Exception,'closed before row release'):
            await anext(stream)
        self.assertEqual(len(calls),1)
        await stream.aclose()
