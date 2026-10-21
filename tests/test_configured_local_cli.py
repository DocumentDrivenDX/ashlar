# SPDX-License-Identifier: MIT OR Apache-2.0
"""Explicit CLI composition and buffered reports; no native runtime acquisition."""
import contextlib,hashlib,io,json,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from ashlar_host import configured_local_cli as command

class Tests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name).resolve();p=lambda name:str(self.root/name)
        self.value={'profile':'ashlar-configured-local/0.2','operation':'initialize','ordinal':0,'python':sys.executable,
            'installation':p('installation'),'installation_id':'configured-test','installation_sha256':None,'output':p('output'),
            'jars':p('jars'),'jar_inventory':{'delta-spark_4.2_2.13-4.4.1.jar':'a'*64,'delta-storage-4.4.1.jar':'b'*64},
            'java_home':p('java'),'source_configuration':p('source.json'),'stream':'original-stream',
            'materialization_clock_us':'1791547200000000','operation_capacity':None,
            'producer':{'source':p('umf'),'bun':p('bun'),'git':p('git'),'timeout_seconds':30,'maximum_output_bytes':1048576,'maximum_receipt_bytes':1048576},
            'bounds':{'whole_seconds':300,'stream_bytes':1048576,'receipt_bytes':262144,'maximum_admission_receipt_bytes':1048576}}
        self.file=self.root/'invocation.json';self.raw=json.dumps(self.value).encode();self.file.write_bytes(self.raw)
    def test_closed_configuration_and_explicit_operation(self):
        config=command.parse_configured_local(self.raw)
        self.assertEqual(config.runtime.admission.output,self.root/'output/admission')
        for change in ({'ordinal':True},{'operation':'replace'},{'extra':'x'}):
            with self.subTest(change=change),self.assertRaises(ValueError):command.parse_configured_local(json.dumps(dict(self.value,**change)).encode())
        with self.assertRaises(ValueError):command.parse_configured_local(b'{"profile":"other",'+self.raw[1:])
        too_large=json.loads(self.raw);too_large['bounds']['stream_bytes']=2097152
        with self.assertRaises(ValueError):command.parse_configured_local(json.dumps(too_large).encode())
    def capture(self,result,status='complete'):
        output=self.root/'output';output.mkdir(mode=0o700)
        if result is not None:(output/'runtime-report.json').write_bytes(command._encoded(result))
        envelope={'profile':'ashlar-configured-local-worker/0.1','configuration_sha256':hashlib.sha256(self.raw).hexdigest(),'status':status,'result':result}
        return command._encoded(envelope),b'private bounded diagnostic'
    def test_owned_worker_closing_precedes_final_report(self):
        answer={'profile':'ashlar-configured-local-initialized/0.2','installation_id':'configured-test','operation':'initialize','installation_sha256':'a'*64,'installation':{},'published':False,'protected_ack':False}
        with patch.object(command,'capture_process',side_effect=lambda *a,**k:self.capture(answer))as capture:
            self.assertEqual(command.run_configured_local_command(self.file),answer)
        config=capture.call_args;self.assertIn('ashlar_host.configured_local_cli',config.args[0])
        self.assertEqual(config.kwargs['timeout_seconds'],300)
        self.assertNotIn('DATABRICKS_HOST',config.kwargs['environment'])
        self.assertEqual(json.loads((self.root/'output/report.json').read_bytes()),answer)
        self.assertEqual((self.root/'output/report.json').stat().st_mode&0o777,0o600)
    def test_read_requires_explicit_selector_and_original_installation_pin(self):
        value=dict(self.value,operation='read',ordinal=1,installation_sha256='a'*64,selector={'type_id':1017,'entity_id':1})
        config=command.parse_configured_local(json.dumps(value).encode())
        self.assertEqual(config.selector,(1017,1))
        for changed in (dict(value,installation_sha256=None),dict(value,selector={'type_id':True,'entity_id':1}),{k:v for k,v in value.items()if k!='selector'}):
            with self.subTest(changed=changed),self.assertRaises(ValueError):command.parse_configured_local(json.dumps(changed).encode())
    def test_export_closed_selection_and_worker_dispatch(self):
        value=dict(self.value,operation='export',ordinal=1,installation_sha256='a'*64)
        raw=json.dumps(value).encode();self.file.write_bytes(raw)
        config=command.parse_configured_local(raw);self.assertEqual(config.operation,'export')
        for changed in (dict(value,installation_sha256=None),dict(value,ordinal=True),dict(value,selector={'type_id':1,'entity_id':1})):
            with self.assertRaises(ValueError):command.parse_configured_local(json.dumps(changed).encode())
        answer={'inert':'dispatch only'}
        with patch('ashlar_host.configured_local_runtime.export_configured_local',return_value=answer)as export,patch.object(command,'_diagnostics',contextlib.nullcontext),patch.object(command.signal,'getsignal',return_value=command.signal.SIG_DFL),patch.object(command.signal,'signal'),patch.object(command.signal,'setitimer'):
            envelope=json.loads(command._worker(self.file,hashlib.sha256(raw).hexdigest()))
        self.assertEqual(envelope['result'],answer);self.assertEqual(export.call_args.kwargs,{'ordinal':1})
    def test_worker_refusal_keeps_diagnostics_and_withholds_report(self):
        with patch.object(command,'capture_process',side_effect=lambda *a,**k:self.capture(None,'refused')):
            with self.assertRaises(ValueError):command.run_configured_local_command(self.file)
        self.assertFalse((self.root/'output/report.json').exists())
        self.assertEqual(json.loads((self.root/'output/failure.json').read_bytes())['status'],'refused')
        self.assertEqual((self.root/'output/diagnostics.log').read_bytes(),b'private bounded diagnostic')
    def test_invocation_drift_withholds_public_report(self):
        def changed(*a,**k):
            result=self.capture({'profile':'runtime-result'});self.file.write_bytes(self.raw+b'\n');return result
        with patch.object(command,'capture_process',side_effect=changed):
            with self.assertRaises(Exception):command.run_configured_local_command(self.file)
        self.assertFalse((self.root/'output/report.json').exists())
    def test_public_cli_dispatch_and_private_failure(self):
        from ashlar.cli import main
        out=io.StringIO();err=io.StringIO()
        with patch.object(sys,'argv',['ashlar','run-configured-local','--configuration',str(self.file)]),patch.object(command,'run_configured_local_command',side_effect=ValueError('private')),contextlib.redirect_stdout(out),contextlib.redirect_stderr(err),self.assertRaises(SystemExit)as result:main()
        self.assertEqual(result.exception.code,2);self.assertEqual(out.getvalue(),'');self.assertEqual(err.getvalue(),'ashlar configured local run refused\n')

    def query_value(self):
        return dict(self.value,operation='query',ordinal=1,installation_sha256='a'*64,query={
            'profile':'ashlar-configured-local-weft/0.1','sql':'SELECT COUNT(*) AS n FROM item',
            'compiler':{'index':str(self.root/'index.json'),'installation':str(self.root/'compiler')},
            'bounds':{'maximum_artifact_bytes':1048576,'maximum_rows':1,'maximum_cell_bytes':65536,'maximum_total_cell_bytes':1048576}})
    def query_answer(self,sql):
        return {'profile':'ashlar-configured-local-query-result/0.1','installation_id':'configured-test','operation':'query','manifest':{},'ordinal':1,'local_checkpoint':'1','completed_batches':1,'protected_ack':False,
            'query':{'sql':sql,'columns':[{'outputName':'n'}],'rows':[['1']],'protocol':{'request':json.dumps({'sql':sql}),'response':'{}','recompiled':'{}'},'evidence':{}}}
    def test_query_closed_typed_selection_and_result_bounds(self):
        from ashlar_host.configured_local_query import ConfiguredLocalQueryConfig
        value=self.query_value();config=command.parse_configured_local(json.dumps(value).encode())
        self.assertIs(type(config.query),ConfiguredLocalQueryConfig);self.assertIsNone(config.selector)
        self.assertEqual(config.query.compile.paths.index,self.root/'index.json')
        for kind in ('missing','selector','extra','limit','profile'):
            changed=json.loads(json.dumps(value))
            if kind=='missing':changed.pop('query')
            elif kind=='selector':changed['selector']={'type_id':1,'entity_id':1}
            elif kind=='extra':changed['query']['compiler']['package']='unexpected'
            elif kind=='limit':changed['query']['bounds']['maximum_rows']=1001
            else:changed['query']['profile']='other'
            with self.subTest(kind=kind),self.assertRaises((ValueError,KeyError)):command.parse_configured_local(json.dumps(changed).encode())
        answer=self.query_answer(config.query.sql);self.assertIs(command._result(config,answer),answer)
        for kind in ('sql','rows','protocol'):
            changed=json.loads(json.dumps(answer))
            if kind=='sql':changed['query']['sql']='changed'
            elif kind=='rows':changed['query']['rows']*=2
            else:changed['query']['protocol']['recompiled']='different'
            with self.subTest(kind=kind),self.assertRaises(ValueError):command._result(config,changed)
    def test_query_selects_the_separate_current_weft_candidate_index(self):
        from ashlar.weft_current_distribution import CurrentWeftDistributionPaths
        value=self.query_value();value['query']['sql']='SELECT p.person_key FROM Person p'
        value['query']['compiler']={'profile':'ashlar-weft-current-candidate/0.1','index':str(self.root/'current-index.json')}
        config=command.parse_configured_local(json.dumps(value).encode())
        self.assertIs(type(config.query.compile.paths),CurrentWeftDistributionPaths)
        self.assertEqual(config.query.compile.paths.index,self.root/'current-index.json')
        value['query']['compiler']['extra']='unselected'
        with self.assertRaises(ValueError):command.parse_configured_local(json.dumps(value).encode())
    def test_query_worker_dispatches_exact_typed_selection(self):
        value=self.query_value();raw=json.dumps(value).encode();self.file.write_bytes(raw);digest=hashlib.sha256(raw).hexdigest()
        answer=self.query_answer(value['query']['sql'])
        with patch('ashlar_host.configured_local_runtime.query_configured_local',return_value=answer)as query,patch.object(command,'_diagnostics',contextlib.nullcontext),patch.object(command.signal,'getsignal',return_value=command.signal.SIG_DFL),patch.object(command.signal,'signal'),patch.object(command.signal,'setitimer'):
            envelope=json.loads(command._worker(self.file,digest))
        self.assertEqual(envelope['result'],answer);self.assertEqual(query.call_count,1)
        self.assertEqual(query.call_args.kwargs['ordinal'],1)
        self.assertEqual(query.call_args.kwargs['query'].sql,value['query']['sql'])
        self.assertIs(type(query.call_args.args[0]),command.ConfiguredLocalRuntimeConfig)
