# SPDX-License-Identifier: MIT OR Apache-2.0
import json,os,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import trace_native_verification as trace

class Tests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.root.chmod(0o700);self.path=self.root/'trace.json'
    def test_nested_counts_and_current_unfinished_call(self):
        observed=[]
        def child():observed.append(json.loads(self.path.read_bytes())['active'])
        def parent():child()
        with patch.object(trace,'installed_bindings',return_value={'ledger':parent,'history':child}):
            with trace.NativeVerificationTrace(self.path):parent()
        result=json.loads(self.path.read_bytes())
        self.assertEqual([x['label']for x in observed[0]],['ledger','history'])
        self.assertEqual(result['counts'],{'ledger':1,'history':1});self.assertEqual(result['active'],[])
        self.assertEqual([e['event']for e in result['events']],['call','call','return','return'])
        self.assertIsNone(sys.getprofile());self.assertEqual(self.path.stat().st_mode&0o777,0o600)
    def test_bounded_history_keeps_active_and_redacts_arguments(self):
        observed=[]
        def child(secret):
            if secret=='SECRET':observed.append(json.loads(self.path.read_bytes())['active'])
        with patch.object(trace,'installed_bindings',return_value={'history':child}):
            with trace.NativeVerificationTrace(self.path):
                for _ in range(100):child('SQL password private/path')
                child('SECRET')
        raw=self.path.read_bytes();result=json.loads(raw)
        self.assertLessEqual(len(raw),trace.MAX_BYTES);self.assertEqual(len(result['events']),trace.MAX_EVENTS)
        self.assertEqual(result['counts']['history'],101);self.assertGreater(result['dropped_events'],0)
        self.assertEqual(observed[0][0]['label'],'history')
        for secret in (b'SECRET',b'SQL',b'password',b'private/path'):self.assertNotIn(secret,raw)
    def test_primary_failure_cancel_and_profile_restore(self):
        def selected():raise KeyboardInterrupt('sensitive')
        with patch.object(trace,'installed_bindings',return_value={'verify':selected}):
            with self.assertRaises(KeyboardInterrupt):
                with trace.NativeVerificationTrace(self.path)as owner:
                    with patch.object(owner,'_write',side_effect=OSError('private secret')):selected()
        self.assertTrue(owner.diagnostic_failed);self.assertIsNone(sys.getprofile())
        for phase in ('callback','closing','primary-cancellation'):
            cancellation=KeyboardInterrupt('private signal');primary=KeyboardInterrupt('original signal');closed=[]
            def function():pass
            with patch.object(trace,'installed_bindings',return_value={'verify':function}):
                with self.assertRaises(KeyboardInterrupt)as caught:
                    with trace.NativeVerificationTrace(self.root/phase)as owner:
                        try:
                            if phase=='callback':
                                with patch.object(owner,'_write',side_effect=cancellation):function()
                            else:
                                def cancelled_write():raise cancellation
                                owner._write=cancelled_write
                                if phase=='primary-cancellation':raise primary
                        finally:closed.append(True)
            self.assertIs(caught.exception,primary if phase=='primary-cancellation'else cancellation)
            self.assertEqual(closed,[True]);self.assertIsNone(owner.fd);self.assertIsNone(sys.getprofile())
        previous=lambda *args:None
        sys.setprofile(previous)
        try:
            with self.assertRaises(RuntimeError):trace.NativeVerificationTrace(self.root/'other').__enter__()
            self.assertIs(sys.getprofile(),previous)
        finally:sys.setprofile(None)
        self.assertFalse((self.root/'other').exists())
