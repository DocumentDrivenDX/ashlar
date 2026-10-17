"""Inert installed composition/worker-probe controls; no SDK or native admission."""
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
import hashlib,json,os,sys,tempfile,unittest
from unittest import mock
from ashlar import cli
from ashlar_host import supply_chain_graphframes_cli as command
from ashlar_host import graphframes_isolated_runtime as isolated
from ashlar_host.graphframes_local import GraphFramesLocalRuntimeSelection,LocalGraphFramesRuntimePolicy
from ashlar_host.graphframes_release import GraphFramesRuntimeConfig,VERSIONS
from ashlar_host.graphframes_runtime_probe import runtime_bytes
from ashlar_host.supply_chain_producer import OriginalSupplyChainDatasetInvocation


class Tests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup);self.root=Path(temporary.name).resolve()
        r=self.root
        for name in ('publication','source','jars','java21','java17','gf-jars','worker'):(r/name).mkdir()
        (r/'java17/bin').mkdir();(r/'java17/bin/java').write_bytes(b'java')
        for name in ('pack','model','graph','bun','git','worker/python'):(r/name).write_bytes(b'{}')
        for name in ('registry.json','operations.sqlite'):(r/'publication'/name).write_bytes(b'inert-never-opened')
        self.config={'profile':'ashlar-original-supply-chain-native-invocation/0.1','output':str(r/'publication'),
            **{k:str(r/k)for k in ('jars','pack','model','graph')},
            'producer':{**{k:str(r/k)for k in ('source','bun','git')},'timeout_seconds':30,'maximum_output_bytes':1048576,'maximum_receipt_bytes':8388608},
            'compiler':{'index':str(r/'absent-unused-index'),'installation':str(r/'absent-unused-compiler')},
            'operation_capacity':{'profile':'ashlar-private-local-operation-capacity/0.1','max_intent_bytes':8388608}}
        self.configuration=r/'native.json';self.configuration.write_text(json.dumps(self.config))
        self.plan=r/'plan.json';self.plan.write_bytes(b'{}');self.plan_sha=hashlib.sha256(b'{}').hexdigest()
        self.output=r/'result';self.events=[];self.source_active=False;self.runtime_active=False
        self.failure=None
    def call(self):
        return command.run_supply_chain_graphframes_command(self.configuration,self.root/'java21',self.plan,
            self.plan_sha,self.root/'java17',self.root/'gf-jars',self.output)
    def composition(self):
        test=self;r=self.root
        @contextmanager
        def source(config):
            self.assertEqual(config.root,r/'publication');self.assertEqual(config.journal,r/'publication/operations.sqlite')
            self.assertEqual(config.runtime.directory.stat().st_mode&0o777,0o700)
            self.source_active=True;self.events.append('source-open')
            try:yield object()
            finally:
                self.assertFalse((self.output/'report.json').exists());self.source_active=False;self.events.append('source-close')
                if self.failure=='source-close':raise PermissionError('source closing refused')
        class Runtime:
            def __init__(self,*args):self.observations=[]
            def check_worker(self):
                self.observations.append({'probe':len(self.observations)})
                if not test.runtime_active:
                    test.runtime_active=True;test.events.append('runtime-open')
                else:
                    test.assertFalse((test.output/'report.json').exists())
                    test.runtime_active=False;test.events.append('runtime-close')
                    if test.failure=='runtime-close':raise PermissionError('runtime closing refused')
        def execute(opened,runtime,policy,context,observe):
            self.assertTrue(self.source_active and self.runtime_active);self.assertIs(policy,context)
            self.assertEqual(runtime.work,self.output/'engine');self.assertEqual(runtime.timeout_seconds,300)
            observe({'kind':'inert-raw-record','schema':[],'rows':[]});self.events.append('engine')
            if self.failure=='cancel':raise KeyboardInterrupt()
            return {'common_cases_passed':7,'cases':[]}
        inventory=SimpleNamespace(worker_revision='a'*40,worker=(),sdks=())
        invocation=object.__new__(OriginalSupplyChainDatasetInvocation)
        producer=SimpleNamespace(original_invocation=lambda:invocation)
        plan={'python':str(r/'worker/python'),'roots':{str(r/'worker'):{} }}
        patches=[mock.patch.object(command,'read_local_runtime_plan',return_value=(b'{}',(1,2),plan,inventory)),
            mock.patch.object(command,'HeldSupplyChainDataset',return_value=producer),
            mock.patch.object(command,'open_supply_chain_common_read',side_effect=source),
            mock.patch.object(command,'execute_supply_chain_graphframes_release',side_effect=execute),
            mock.patch.object(isolated,'IsolatedGraphFramesRuntimePolicy',Runtime)]
        for patch in patches:patch.start();self.addCleanup(patch.stop)
    def test_real_context_composition_closes_both_owners_before_private_report(self):
        self.composition();before={k:os.environ.get(k)for k in ('JAVA_HOME','SPARK_LOCAL_IP')}
        answer=self.call();value=json.loads((self.output/'report.json').read_bytes())
        self.assertEqual(self.events,['runtime-open','source-open','engine','source-close','runtime-close'])
        self.assertEqual(len(value['runtime_observations']),2);self.assertEqual(len(value['observations']),1)
        self.assertEqual(answer['common_cases'],7);self.assertEqual((self.output/'report.json').stat().st_mode&0o777,0o600)
        self.assertEqual(before,{k:os.environ.get(k)for k in before})
        # Compiler paths are intentionally absent: this query neither opens Weft
        # nor invokes a publisher; the actual source/native factories own admission.
        self.assertFalse((self.root/'absent-unused-index').exists())
    def test_cancellation_and_both_closing_failures_withhold_command_report(self):
        self.composition()
        for ordinal,failure in enumerate(('cancel','source-close','runtime-close')):
            self.output=self.root/('result'+str(ordinal));self.failure=failure;self.events=[]
            with self.subTest(failure=failure),self.assertRaises((KeyboardInterrupt,PermissionError)):self.call()
            self.assertFalse((self.output/'report.json').exists())
            self.assertFalse(self.source_active or self.runtime_active)
            self.assertIn('source-close',self.events);self.assertIn('runtime-close',self.events)
    def test_actual_probe_argv_environment_bounds_and_correspondence_refusal(self):
        r=self.root;selection=GraphFramesLocalRuntimeSelection(r/'source',self.plan,self.plan_sha)
        runtime=GraphFramesRuntimeConfig(r/'worker/python','a'*64,r/'java17','b'*64,'c'*40,'d'*64,'e'*64,r/'gf-jars',r/'work')
        with mock.patch('ashlar_host.graphframes_local.read_local_runtime_plan',return_value=(b'{}',(1,2),{},object())):
            self.assertIs(LocalGraphFramesRuntimePolicy(selection).config,selection)
        owner=object.__new__(isolated.IsolatedGraphFramesRuntimePolicy)
        owner.selection=selection;owner.runtime=runtime;owner.inventory=SimpleNamespace(worker_revision='c'*40)
        owner.runtime_raw=runtime_bytes(runtime);owner.runtime_path=r/'runtime.json';owner.observations=[];owner.probes=0
        reply={'profile':'ashlar-current-isolated-graphframes-runtime/0.1','runtime_sha256':hashlib.sha256(owner.runtime_raw).hexdigest(),
            'runtime_plan_sha256':self.plan_sha,'python':str(runtime.python),'worker_revision':'c'*40,
            'versions':VERSIONS,'constructor':'graphframes.classic.graphframe.GraphFrame','numpy_version':'selected'}
        with mock.patch.object(owner,'renew_graphframes_runtime')as renew,mock.patch.object(isolated,'capture_graphframes_worker',return_value=(json.dumps(reply).encode(),b''))as capture:
            owner.check_worker();args=capture.call_args.args[0];kw=capture.call_args.kwargs
            self.assertEqual(args[:5],[str(runtime.python),'-I','-B','-m','ashlar_host.graphframes_runtime_probe'])
            self.assertEqual(kw['timeout_seconds'],30);self.assertEqual(kw['maximum_output_bytes'],65536)
            self.assertNotIn('PYTHONPATH',kw['environment']);self.assertEqual(renew.call_count,2)
            capture.return_value=(json.dumps(dict(reply,python='/another/python')).encode(),b'')
            with self.assertRaises(PermissionError):owner.check_worker()
            self.assertEqual(len(owner.observations),1)
    def test_cli_dispatch_and_runtime_inspection_file_are_explicit(self):
        argv=['ashlar','query-supply-chain-graphframes']
        for name in ('configuration','java-home','runtime-plan','runtime-plan-sha256','graphframes-java-home','graphframes-jars','output'):argv.extend(['--'+name,name])
        with mock.patch.object(sys,'argv',argv),mock.patch.object(command,'run_supply_chain_graphframes_command',return_value={'output':'selected'})as dispatch,mock.patch('builtins.print'):
            cli.main()
        self.assertEqual(dispatch.call_args.args,('configuration','java-home','runtime-plan','runtime-plan-sha256','graphframes-java-home','graphframes-jars','output'))
        raw=b'{"candidate":true}';target=self.root/'candidate.json'
        with mock.patch('ashlar_host.graphframes_local_setup.inspect_local_graphframes_runtime',return_value=raw):
            result=command.run_inspect_graphframes_runtime_command(target,'a'*40,(self.root/'worker',),self.root/'jars',(self.root/'java17',))
        self.assertEqual(target.read_bytes(),raw);self.assertEqual(result['sha256'],hashlib.sha256(raw).hexdigest())
