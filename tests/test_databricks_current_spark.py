"""Concrete Spark mechanics with inert native-shaped ports; no authority claim."""
import contextlib
import hashlib
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from ashlar_host.databricks_current_spark import SparkCurrentQueryBackend, CurrentSparkCapture
from ashlar_host.databricks_current_query import execute_databricks_current_query
from ashlar_host.databricks_current_source_capture import CurrentSourceCapture
from ashlar_host import databricks_current_source_capture as source_capture_owner
from ashlar_host.path_capture import PathCaptureConfig


def text(value):return json.dumps(value,separators=(',',':'))
def sha(raw):return hashlib.sha256(raw.encode()).hexdigest()


class Parent: pass
class Child:
    def __init__(self):self.parent=Parent();self.checks=0;self.bad_report=False
    def source_preparation(self):return self.parent
    def lower_databricks_current_contributors_candidate_json(self,binding):
        return text({'version':'weft.databricks.current-scalar/0.1-candidate',
            'authority':False,'securityReady':False,'nativeAdmission':'required',
            'sourceRequestSha256':'a'*64,'designationSha256':'b'*64,
            'materializationBindingSha256':sha(binding),'sql':'UNCHANGED :wft_1 :wft_2',
            'parameters':[{'ordinal':1,'marker':'wft_1','value':'-1','kind':'source-value','nativeType':'BIGINT'},
                          {'ordinal':2,'marker':'wft_2','value':'s0','kind':'internal-lineage-label','nativeType':'STRING'}],
            'resultColumns':[{'position':0,'name':'duplicate'},{'position':1,'name':'duplicate'}],
            'privateResultColumns':[{'position':2,'name':'__weft_contributors','purpose':'source-row-contributor-bag','encoding':'json-text:weft.stage-contributor-bag/0.1-candidate','multiplicity':'bag','maxKeyUtf8Bytes':256}]})
    def check_databricks_current_contributors_candidate_json(self,binding,frame):
        self.checks+=1;v=json.loads(frame)
        if v['resultRows']!=[['left','right','private-carrier']]:raise ValueError('conditional mismatch')
        return text({'version':'weft.current-query.refinement-result/0.1-candidate',
            'matched':True,'authority':False,'securityReady':False,'nativeAdmission':'required',
            'sourceRequestSha256':'a'*64,'designationSha256':'b'*64,
            'materializationBindingSha256':sha(binding),
            'compiledArtifactSha256':sha(self.lower_databricks_current_contributors_candidate_json(binding)),
            'refinementInputSha256':'0'*64 if self.bad_report else sha(frame)})


class Frame:
    def __init__(self,rows,events,bad_schema=False):
        self.rows=rows;self.events=events
        self.schema=SimpleNamespace(fields=[SimpleNamespace(name=name,nullable=False,
            dataType=SimpleNamespace(simpleString=lambda:'BIGINT' if bad_schema else 'STRING'))
            for name in ['output_0','output_1','__weft_contributors']])
    def toLocalIterator(self,prefetchPartitions):
        assert prefetchPartitions is False
        try:yield from self.rows
        finally:self.events.append('iterator-close')


class Authority:
    def __init__(self):self.events=[]
    @contextlib.contextmanager
    def open_current_query(self,child,context):
        self.events.append('open')
        try:yield self
        finally:self.events.append('close')
    def binding(self):return '{}'
    def admit(self,*args):self.events.append('admit')
    def renew(self,*args):self.events.append('renew')


class Tests(unittest.TestCase):
    def setup_ports(self,rows=None,bad_schema=False):
        self.authority=Authority();self.child=Child();self.context=object();self.calls=[]
        def sql(statement,*,args):
            self.calls.append((statement,args))
            return Frame([['left','right','private-carrier']] if rows is None else rows,
                         self.authority.events,bad_schema)
        limits=PathCaptureConfig(4,1024,4096)
        self.backend=SparkCurrentQueryBackend(SimpleNamespace(sql=sql),self.authority,limits,limits)
        return contextlib.ExitStack()

    def invoke(self,**kwargs):
        native=SimpleNamespace(CurrentQueryPreparation=Child,ApplicationLineagePreparation=Parent)
        with patch('ashlar_host.databricks_current_spark.importlib.import_module',return_value=native), \
             patch.object(source_capture_owner,'capture_current_query_sources',
                   return_value=CurrentSourceCapture('{}','[]')):
            return execute_databricks_current_query(self.child,self.context,self.backend,**kwargs)

    def test_unchanged_sql_parameters_and_positional_public_projection_always_checked(self):
        self.setup_ports();result=self.invoke(refinement=False)
        self.assertEqual(self.calls,[('UNCHANGED :wft_1 :wft_2',{'wft_1':'-1','wft_2':'s0'})])
        self.assertEqual(json.loads(result.verified_result),{'columns':['duplicate','duplicate'],'rows':[['left','right']]})
        self.assertEqual(self.child.checks,1)
        self.assertEqual(self.authority.events[-1],'close')
        self.assertIn('iterator-close',self.authority.events)

    def test_checker_mismatch_or_wrong_report_never_releases_and_closes(self):
        for bad_report in (False,True):
            self.setup_ports(rows=None if bad_report else [['wrong','right','private-carrier']])
            self.child.bad_report=bad_report
            with self.assertRaises(ValueError):self.invoke()
            self.assertEqual(self.child.checks,1)
            self.assertEqual(self.authority.events[-1],'close')

    def test_native_schema_or_capacity_refuses_before_checker(self):
        for bad_schema in (False,True):
            self.setup_ports(rows=[['x'*1025,'right','private-carrier']],bad_schema=bad_schema)
            with self.assertRaises(ValueError):self.invoke()
            self.assertEqual(self.child.checks,0)
            self.assertEqual(self.authority.events[-1],'close')

    def test_checker_capability_and_structural_cell_budget_before_sql(self):
        self.setup_ports()
        with patch.object(Child,'check_databricks_current_contributors_candidate_json',None):
            with self.assertRaises(ValueError):self.invoke(refinement=False)
        self.assertEqual(self.calls,[])
        self.assertEqual(self.authority.events,[])
        with self.assertRaises(ValueError):
            SparkCurrentQueryBackend(self.backend.spark,self.authority,self.backend.capture_limits,PathCaptureConfig(4,65537,100000))
        self.assertEqual(self.calls,[])
        self.backend=SparkCurrentQueryBackend(self.backend.spark,self.authority,
            PathCaptureConfig(10000,1024,4096),self.backend.source_limits)
        original=Child.lower_databricks_current_contributors_candidate_json
        def wider(child,binding):
            artifact=json.loads(original(child,binding))
            artifact['resultColumns']=[{'position':i,'name':'same'} for i in range(10)]
            artifact['privateResultColumns'][0]['position']=10
            return text(artifact)
        with patch.object(Child,'lower_databricks_current_contributors_candidate_json',wider):
            with self.assertRaises(ValueError):self.invoke()
        self.assertEqual(self.calls,[])
        self.assertEqual(self.authority.events[-1],'close')

    def test_source_capture_cancellation_preserved_through_failing_renewal_and_close(self):
        self.setup_ports();cancel=KeyboardInterrupt()
        native=SimpleNamespace(CurrentQueryPreparation=Child,ApplicationLineagePreparation=Parent)
        def renewal(*args):
            self.authority.events.append('renew')
            if self.child.checks==0 and 'iterator-close' in self.authority.events:
                raise OSError('secondary closing failure')
        # Source failure is the primary; later authority closing failure cannot replace it.
        def source(*args,**kwargs):
            self.authority.renew=renewal
            raise cancel
        with patch.object(source_capture_owner,'capture_current_query_sources',side_effect=source), \
             patch('ashlar_host.databricks_current_spark.importlib.import_module',return_value=native):
            with self.assertRaises(KeyboardInterrupt) as raised:
                execute_databricks_current_query(self.child,self.context,self.backend)
        self.assertIs(raised.exception,cancel)
        self.assertEqual(self.authority.events[-1],'close')
        self.assertEqual(self.child.checks,0)

    def test_foreign_capture_cannot_borrow_held_identity(self):
        self.setup_ports();native=SimpleNamespace(CurrentQueryPreparation=Child,ApplicationLineagePreparation=Parent)
        with patch('ashlar_host.databricks_current_spark.importlib.import_module',return_value=native):
            with self.backend.open_current_query(self.child,self.context) as held:
                artifact=self.child.lower_databricks_current_contributors_candidate_json('{}')
                captured=held.capture(artifact)
                with self.assertRaises(ValueError):
                    held.verify(self.child,artifact,CurrentSparkCapture(captured.native_json),self.context)
                with self.assertRaises(ValueError):held.capture(artifact)
                object.__setattr__(captured,'native_json',text({'rows':[['forged']]}))
                with self.assertRaises(ValueError):held.verify(self.child,artifact,captured,self.context)
        self.assertEqual(len(self.calls),1)
        self.assertEqual(self.authority.events[-1],'close')
        for action in (held.binding, lambda:held.capture(artifact),
                lambda:held.refinement_input(self.child,artifact,captured,self.context),
                lambda:held.verify(self.child,artifact,captured,self.context)):
            with self.assertRaises(ValueError):action()


if __name__=='__main__':unittest.main()
