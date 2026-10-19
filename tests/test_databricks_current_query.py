# SPDX-License-Identifier: MIT OR Apache-2.0
"""Inert owner lifecycle controls; not native security or engine qualification."""
import contextlib
import hashlib
import json
import types
import unittest
from unittest.mock import patch
from ashlar_host.databricks_current_query import execute_databricks_current_query


class Parent: pass
class Child:
    def __init__(self): self.parent=Parent()
    def source_preparation(self): return self.parent
    def lower_databricks_current_contributors_candidate_json(self,binding):
        return json.dumps({'version':'weft.databricks.current-scalar/0.1-candidate','authority':False,'securityReady':False,'nativeAdmission':'required',
            'materializationBindingSha256':hashlib.sha256(binding.encode()).hexdigest(),
            'sql':'ORIGINAL SQL','parameters':[{'value':'exact'}]})
    def lower_databricks_current_sum_candidate_json(self,binding):
        value=json.loads(self.lower_databricks_current_contributors_candidate_json(binding))
        value['version']='weft.databricks.current-scalar/0.2-candidate'
        return json.dumps(value)


class Backend:
    def __init__(self,fail=None): self.events=[];self.fail=fail;self.child=None
    @contextlib.contextmanager
    def open_current_query(self,child,context):
        self.child=child;self.events.append('open')
        try:yield self
        finally:
            self.events.append('close')
            if self.fail=='close':raise RuntimeError('closing refusal')
    def binding(self):return '{}'
    def admit(self,child,binding,context):
        assert child is self.child and binding=='{}';self.events.append('admit')
        if self.fail=='admit':raise PermissionError('no authority')
    def renew(self,*args):self.events.append('renew')
    def capture(self,original):
        self.events.append('capture');self.original=original
        if self.fail in ('capture','cancel'):
            raise (KeyboardInterrupt() if self.fail=='cancel' else LookupError('primary'))
        self.captured=object();return self.captured
    def verify(self,child,original,captured,context):
        assert original is self.original;self.events.append('verify');return '{"rows":[]}'


class CurrentQueryTests(unittest.TestCase):
    def invoke(self,backend,**kwargs):
        # Only this inert transport test replaces the optional import; production
        # always checks exact native installed classes and their descriptors.
        native=types.SimpleNamespace(CurrentQueryPreparation=Child,ApplicationLineagePreparation=Parent)
        with patch('ashlar_host.databricks_current_query.importlib.import_module',return_value=native):
            return execute_databricks_current_query(Child(),object(),backend,**kwargs)
    def test_original_artifact_and_closing_before_release(self):
        b=Backend();result=self.invoke(b)
        self.assertEqual(b.events,['open','admit','renew','capture','verify','renew','admit','close'])
        self.assertIs(result.original_artifact,b.original);self.assertFalse(result.authority)
        self.assertEqual(json.loads(result.verified_result),{'rows':[]})
    def test_admission_and_closing_refuse_release(self):
        for failure,exception in [('admit',PermissionError),('close',RuntimeError)]:
            b=Backend(failure)
            with self.assertRaises(exception):self.invoke(b)
            if failure=='admit':self.assertNotIn('capture',b.events)
        with self.assertRaises(ValueError):self.invoke(Backend(),maximum_result_bytes=True)
    def test_foreign_child_refused_before_backend(self):
        b=Backend();native=types.SimpleNamespace(CurrentQueryPreparation=Child)
        with patch('ashlar_host.databricks_current_query.importlib.import_module',return_value=native):
            with self.assertRaises(TypeError):execute_databricks_current_query({},object(),b)
        self.assertEqual(b.events,[])

    def test_explicit_spark_requires_checker_before_open(self):
        b=Backend()
        with self.assertRaises((ValueError,AttributeError)):self.invoke(b,engine='spark')
        self.assertEqual(b.events,[])
        def lower(child,binding):
            result=json.loads(child.lower_databricks_current_contributors_candidate_json(binding))
            result['version']='weft.spark.current-scalar/0.1-candidate'
            return json.dumps(result)
        with patch.object(Child,'lower_spark_current_contributors_candidate_json',lower,create=True), \
             patch.object(Child,'check_spark_current_contributors_candidate_json',lambda *a:None,create=True):
            result=self.invoke(Backend(),engine='spark')
            self.assertEqual(json.loads(result.original_artifact)['version'],'weft.spark.current-scalar/0.1-candidate')
        with self.assertRaises(ValueError):self.invoke(Backend(),engine='spark',mode='sum')

    def test_failure_and_cancellation_still_close(self):
        for kind,exception in [('capture',LookupError),('cancel',KeyboardInterrupt)]:
            b=Backend(kind)
            with self.assertRaises(exception):self.invoke(b)
            self.assertEqual(b.events[-3:],['renew','admit','close'])
        b=Backend('capture')
        original=b.renew
        def renewal(*args):
            original(*args)
            if 'capture' in b.events:raise RuntimeError('secondary')
        b.renew=renewal
        with self.assertRaises(LookupError):self.invoke(b)
        self.assertIn('admit',b.events)
    def test_strict_public_result_json(self):
        for raw in ['{"x":1,"x":2}','{"x":NaN}','{"x":1e999}']:
            b=Backend();b.verify=lambda *args:raw
            with self.assertRaises(ValueError):self.invoke(b)
            self.assertEqual(b.events[-1],'close')
        b=Backend()
        with self.assertRaises(ValueError):self.invoke(b,maximum_result_bytes=1)
        b=Backend();b.binding=lambda:'{"x":1,"x":2}'
        with self.assertRaises(ValueError):self.invoke(b)
        self.assertNotIn('capture',b.events)

    def test_sum_and_verification_refusal(self):
        result=self.invoke(Backend(),mode='sum')
        self.assertEqual(json.loads(result.original_artifact)['version'],
                         'weft.databricks.current-scalar/0.2-candidate')
        b=Backend()
        def refuse(*args):raise PermissionError('verification refused')
        b.verify=refuse
        with self.assertRaises(PermissionError):self.invoke(b)
        self.assertEqual(b.events[-3:],['renew','admit','close'])
    def test_binding_drift_before_and_after_capture(self):
        for after_capture in (False,True):
            b=Backend();calls=[0]
            def binding():
                calls[0]+=1
                return '{}' if calls[0] < (3 if after_capture else 2) else '{"changed":true}'
            b.binding=binding
            with self.assertRaises(ValueError):self.invoke(b)
            self.assertEqual('capture' in b.events,after_capture)
            self.assertEqual(b.events[-1],'close')
    def test_context_cannot_suppress_cancellation(self):
        class Suppressing(Backend):
            @contextlib.contextmanager
            def open_current_query(self,child,context):
                self.child=child;self.events.append('open')
                try:yield self
                except BaseException:self.events.append('suppressed')
                finally:self.events.append('close')
        b=Suppressing('cancel')
        with self.assertRaises(KeyboardInterrupt):self.invoke(b)
        self.assertEqual(b.events[-4:],['renew','admit','suppressed','close'])


class RefinedChild(Child):
    def lower_databricks_current_contributors_candidate_json(self,binding):
        value=json.loads(super().lower_databricks_current_contributors_candidate_json(binding))
        value.update(sourceRequestSha256='a'*64,designationSha256='b'*64)
        self.artifact=json.dumps(value);self.binding_original=binding
        return self.artifact
    def lower_databricks_current_sum_candidate_json(self,binding):
        value=json.loads(self.lower_databricks_current_contributors_candidate_json(binding))
        value['version']='weft.databricks.current-scalar/0.2-candidate'
        self.artifact=json.dumps(value);return self.artifact
    def check_databricks_current_contributors_candidate_json(self,binding,frame):
        assert binding is self.binding_original
        self.checked=('contributors',binding,frame)
        value=json.loads(self.artifact)
        return json.dumps({'version':'weft.current-query.refinement-result/0.1-candidate',
            'matched':True,'authority':False,'securityReady':False,'nativeAdmission':'required',
            'sourceRequestSha256':value['sourceRequestSha256'],'designationSha256':value['designationSha256'],
            'materializationBindingSha256':hashlib.sha256(binding.encode()).hexdigest(),
            'compiledArtifactSha256':hashlib.sha256(self.artifact.encode()).hexdigest(),
            'refinementInputSha256':hashlib.sha256(frame.encode()).hexdigest(),**self.changes})
    def check_databricks_current_sum_candidate_json(self,binding,frame):
        result=self.check_databricks_current_contributors_candidate_json(binding,frame)
        self.checked=('sum',binding,frame);return result


class RefinementTests(unittest.TestCase):
    def invoke(self,b,*,mode='contributors',changes=None,cls=RefinedChild):
        child=cls();child.changes=changes or {};context=object();frame=' {"original":"frame"} '
        def refinement_input(actual,artifact,captured,ctx):
            self.assertIs(actual,child);self.assertIs(ctx,context);self.assertIs(artifact,b.original);self.assertIs(captured,b.captured)
            b.events.append('refinement');return frame
        b.refinement_input=refinement_input
        native=types.SimpleNamespace(CurrentQueryPreparation=cls,ApplicationLineagePreparation=Parent)
        with patch('ashlar_host.databricks_current_query.importlib.import_module',return_value=native):
            result=execute_databricks_current_query(child,context,b,mode=mode,refinement=True)
        return result,child,frame
    def test_opt_in_same_original_and_sum_checker(self):
        for mode in ('contributors','sum'):
            b=Backend();result,child,frame=self.invoke(b,mode=mode)
            self.assertEqual(child.checked,(mode,result.original_binding,frame))
            self.assertLess(b.events.index('refinement'),b.events.index('verify'))
            self.assertTrue(json.loads(result.refinement_report)['matched'])
            self.assertFalse(result.authority)
    def test_missing_checker_and_report_refusals(self):
        b=Backend()
        with self.assertRaises(ValueError):self.invoke(b,cls=Child)
        self.assertEqual(b.events,[])
        for changes in [{'refinementInputSha256':'c'*64},{'compiledArtifactSha256':'c'*64},
                        {'sourceRequestSha256':'c'*64},{'authority':True},{'matched':False},{'extra':None}]:
            b=Backend()
            with self.subTest(changes=changes),self.assertRaises(ValueError):self.invoke(b,changes=changes)
            self.assertNotIn('verify',b.events);self.assertEqual(b.events[-3:],['renew','admit','close'])
    def test_refinement_cancellation_keeps_closing(self):
        class CancelChild(RefinedChild):
            def check_databricks_current_contributors_candidate_json(self,*args):raise KeyboardInterrupt()
        b=Backend()
        with self.assertRaises(KeyboardInterrupt):self.invoke(b,cls=CancelChild)
        self.assertNotIn('verify',b.events);self.assertEqual(b.events[-3:],['renew','admit','close'])
