import importlib.util
from pathlib import Path
import tempfile
import time
import signal
import json
from unittest.mock import patch
import unittest
spec=importlib.util.spec_from_file_location('diagnostic',Path(__file__).with_name('diagnose.py'));d=importlib.util.module_from_spec(spec);spec.loader.exec_module(d)
from ashlar_host.fabric_cli_runtime import FabricInertCliRuntimeSelection,FabricRuntimeCustody
class Controls(unittest.TestCase):
    def test_real_inert_profile_reports_io_without_inventory(self):
        with tempfile.TemporaryDirectory(dir='/private/tmp')as directory:
            root=Path(directory).resolve();exe=root/'inert';exe.write_bytes(b'payload')
            selected=FabricInertCliRuntimeSelection(root,exe,())
            report,status=d.profile_once(selected,selected,None,time.monotonic()+2)
            self.assertEqual(status,0)
            self.assertEqual(report['stage']['constructors_returned'],['cli','http'])
            self.assertGreaterEqual(report['costs']['file_open']['total_calls'],6)
            self.assertGreater(report['costs']['file_read']['total_calls'],0)
            self.assertGreater(report['costs']['path_resolution']['total_calls'],0)
            self.assertNotIn(str(exe),str(report));self.assertNotIn('payload',str(report))
    def test_constructor_timeout_stage_and_safe_outcome(self):
        with tempfile.TemporaryDirectory(dir='/private/tmp')as directory:
            root=Path(directory).resolve();exe=root/'inert';exe.write_bytes(b'x')
            report,status=d.profile_once(FabricInertCliRuntimeSelection(root,exe,()),None,None,time.monotonic()-1)
            self.assertEqual(status,2)
            self.assertEqual(report['stage'],{'constructors_started':['cli'],'constructors_returned':[]})
            self.assertEqual(report['original_outcome']['failure'],'deadline-refusal')
            self.assertEqual(report['original_outcome']['closing'],[])
    def test_primary_refusal_preserved_independent_close(self):
        calls=[]
        class Owner:
            original=()
            def close(self):calls.append('closed');raise RuntimeError('sensitive secondary suppressed')
        def factory(selection,**kwargs):
            if selection=='second':raise PermissionError('Unselected runtime link')
            return Owner()
        report,status=d.profile_once('first','second',None,time.monotonic()+2,factory)
        self.assertEqual(status,2);self.assertEqual(calls,['closed'])
        self.assertEqual(report['original_outcome']['failure'],'runtime-link-refusal')
        self.assertNotIn('sensitive',str(report))
    def test_external_signal_during_internal_timeout_handling_retains_profile(self):
        cancelled=[]
        def interrupted(*args):
            try:raise TimeoutError('internal private timeout')
            except TimeoutError:
                time.sleep(0.05) # Independent deadline arrives while handling.
        def alarm(signum,frame):raise TimeoutError('Runtime closure deadline exceeded')
        def cancel():signal.setitimer(signal.ITIMER_REAL,0);cancelled.append(True)
        previous=signal.signal(signal.SIGALRM,alarm)
        try:
            with patch.object(d.recipe,'qualify',interrupted):
                signal.setitimer(signal.ITIMER_REAL,0.005)
                report,status=d.profile_once(None,None,None,time.monotonic()+0.005,cancel_timer=cancel)
            self.assertEqual(status,2);self.assertEqual(cancelled,[True])
            self.assertEqual(signal.getitimer(signal.ITIMER_REAL),(0.0,0.0))
            self.assertEqual(report['original_outcome']['status'],'diagnostic-interrupted')
            self.assertEqual(report['original_outcome']['closing_status'],'unavailable-interrupted-original-recipe')
            self.assertGreater(sum(r['total_calls']for r in report['costs'].values()),0)
            exported=json.dumps(report)
            self.assertNotIn('internal private',exported);self.assertLess(len(exported),16384)
        finally:signal.setitimer(signal.ITIMER_REAL,0);signal.signal(signal.SIGALRM,previous)
    def test_keyboard_interrupt_safe_classification_and_timer_cancellation(self):
        cancelled=[]
        with patch.object(d.recipe,'qualify',side_effect=KeyboardInterrupt('private material')):
            report,status=d.profile_once(None,None,None,time.monotonic()+2,cancel_timer=lambda:cancelled.append(True))
        self.assertEqual(cancelled,[True]);self.assertEqual(status,2)
        self.assertEqual(report['original_outcome']['exception_type'],'KeyboardInterrupt')
        self.assertNotIn('private material',json.dumps(report))
    def test_alarm_cancelled_before_delayed_failure_classification(self):
        calls=[];original=d.recipe.classification
        def classifier(error):
            self.assertEqual(signal.getitimer(signal.ITIMER_REAL),(0.0,0.0))
            calls.append('classification');time.sleep(0.02);return original(error)
        def alarm(signum,frame):raise TimeoutError('Runtime closure deadline exceeded')
        def cancel():signal.setitimer(signal.ITIMER_REAL,0);calls.append('cancel')
        previous=signal.signal(signal.SIGALRM,alarm)
        try:
            with patch.object(d.recipe,'qualify',side_effect=PermissionError('private failure')),patch.object(d.recipe,'classification',classifier):
                signal.setitimer(signal.ITIMER_REAL,0.005)
                report,status=d.profile_once(None,None,None,time.monotonic()+0.005,cancel_timer=cancel)
            self.assertEqual(status,2);self.assertEqual(calls,['cancel','classification'])
            self.assertGreater(sum(r['total_calls']for r in report['costs'].values()),0)
            self.assertNotIn('private failure',json.dumps(report))
        finally:signal.setitimer(signal.ITIMER_REAL,0);signal.signal(signal.SIGALRM,previous)
if __name__=='__main__':unittest.main()
