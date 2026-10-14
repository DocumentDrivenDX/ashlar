"""Fake runtime boundary controls; real small preparation, no PuppyGraph launch."""
import unittest,json,tempfile,hashlib
from pathlib import Path
from contextlib import contextmanager
from unittest.mock import patch
from types import SimpleNamespace
import test_puppy_scenario_preparation as original_fixture
from ashlar.graph_release import GraphRelease
from ashlar_host.finite_pack import FinitePackDefinition
from ashlar_host.finite_dataset import FinitePackSource
from ashlar_host.local_graph_export import LocalGraphExportUse
from ashlar_host.puppy_scenario_preparation import PuppyScenarioPreparationConfig,prepare_puppy_scenarios
from ashlar_host.puppy_finite_local import PuppyFiniteLoopbackConfig,PuppyLocalRuntime,open_puppy_scenario_local

class Tests(unittest.TestCase):
    def fixture(self):
        try:import duckdb
        except ImportError:self.skipTest('Optional selected DuckDB unavailable')
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup);root=Path(temp.name).resolve();root.chmod(0o700)
        sources=[];exports=[];packs=[]
        for name in ('archaeology','ecology'):
            model,graph,pack,raw=original_fixture.Tests.selected(self,name);context=object()
            source=object.__new__(FinitePackSource);source.definition=FinitePackDefinition(name);source.model=model;source.graph=graph;source.context=context
            export=object.__new__(LocalGraphExportUse);export._context=context;export._release=GraphRelease(raw,hashlib.sha256(raw).hexdigest());export._custody_bytes=b'inert';export._custody_sha256='0'*64;export._source=object()
            sources.append(source);exports.append(export);packs.append(pack)
        @contextmanager
        def interval(owner,context):yield
        self.enterContext(patch.object(LocalGraphExportUse,'writer',interval))
        self.enterContext(patch.object(LocalGraphExportUse,'admit_original_export',lambda *args:None))
        self.enterContext(patch.object(FinitePackSource,'metadata',lambda source:source.definition.inputs(source.model,source.graph)))
        prepared=root/'prepared';receipt=prepare_puppy_scenarios(PuppyScenarioPreparationConfig(tuple(packs),prepared),sources=tuple(sources),exports=tuple(exports))
        config=PuppyFiniteLoopbackConfig(Path('/usr/bin/false'),'unix:///explicit.sock','sha256:'+'a'*64,'ashlar-scenario-test',prepared,'private-name','private-password',18081,17687,120)
        return config,tuple(packs),tuple(sources),tuple(exports),receipt
    def test_tampered_mapping_refuses_before_docker_even_repinned(self):
        config,packs,sources,exports,receipt=self.fixture();path=config.prepared/'model.json';path.chmod(0o600);value=json.loads(path.read_bytes());value['node'][0]['label']='Changed';raw=json.dumps(value).encode();path.write_bytes(raw)
        receipt['model_bytes']=len(raw);receipt['model_sha256']=hashlib.sha256(raw).hexdigest();p=config.prepared/'receipt.json';p.chmod(0o600);p.write_text(json.dumps(receipt));p.chmod(0o444);path.chmod(0o444)
        with patch.object(PuppyLocalRuntime,'_docker')as docker:
            with self.assertRaises(ValueError):
                with open_puppy_scenario_local(config,pack_bytes=packs,sources=sources,exports=exports,preparation=receipt,native_map_profile=None):self.fail('No acquisition')
        docker.assert_not_called()
    def test_ready_order_single_post_original_parameters_and_cleanup(self):
        from ashlar_host.pack_scenario_queries import query_plan
        config,packs,sources,exports,receipt=self.fixture();actions=[];cancel=KeyboardInterrupt()
        def docker(owner,args,**kw):
            actions.append(args[0])
            if args[:2]==['image','inspect']:return json.dumps([{'Id':config.image}]).encode()
            if args[0]=='create':return ('b'*64).encode()
            return b''
        def http(owner,path,body=None):
            if body is not None:actions.append('POST');return b'{"ok":true}'
            return json.dumps(owner.requested_model).encode()
        class Session:
            def run(self,q,parameters):actions.append(('parameters',parameters));return [SimpleNamespace(data=lambda:{'result_0':'x'})]*2
            def close(self):actions.append('session-close')
        class Driver:
            def session(self):return Session()
            def close(self):actions.append('driver-close')
        sdk=SimpleNamespace(Query=lambda script,timeout:script,GraphDatabase=SimpleNamespace(driver=lambda *a,**kw:Driver()))
        with patch.object(PuppyLocalRuntime,'_docker',docker),patch.object(PuppyLocalRuntime,'_container',lambda owner:None),patch.object(PuppyLocalRuntime,'_http',http),patch.object(PuppyLocalRuntime,'_wait_ready',lambda owner:actions.append('HTTP-ready')),patch.object(PuppyLocalRuntime,'_wait_backend_ready',lambda owner:actions.append('Bolt-ready')),patch.dict('sys.modules',{'neo4j':sdk}):
            with self.assertRaises(KeyboardInterrupt)as caught:
                with open_puppy_scenario_local(config,pack_bytes=packs,sources=sources,exports=exports,preparation=receipt,native_map_profile=None,maximum_rows=1,maximum_bytes=1048576)as owner:
                    with owner.hold(owner.context):
                        profile=receipt['reports'][0]['profile'];script,bindings,_=query_plan(profile,profile['cases'][0]['original_scenario']['id'],'Cypher')
                        with self.assertRaises(PermissionError):owner.query_original(script,{'invented':1},owner.context)
                        with self.assertRaises(ValueError):owner.query_original(script,bindings,owner.context)
                        raise cancel
        self.assertIs(caught.exception,cancel);self.assertLess(actions.index('HTTP-ready'),actions.index('Bolt-ready'));self.assertLess(actions.index('Bolt-ready'),actions.index('POST'));self.assertEqual(actions.count('POST'),1);self.assertIn('stop',actions);self.assertIn('rm',actions);self.assertIn(('parameters',bindings),actions)
        self.assertNotIn('private-password',repr(config));self.assertNotIn('private-name',repr(config))

    def test_sdk_long_sizing_preserves_original_values(self):
        from ashlar_host.puppy_finite_local import _scenario_sdk_size
        class SDKLong(int):pass
        class UnknownInt(int):pass
        rows=[{'result_0':'x','result_1':SDKLong(7)}]
        with patch.dict('sys.modules',{'gremlin_python.statics':SimpleNamespace(long=SDKLong)}):
            self.assertGreater(_scenario_sdk_size(rows),0)
            with self.assertRaises(ValueError):_scenario_sdk_size(rows,1)
            self.assertIs(type(rows[0]['result_1']),SDKLong)
            with self.assertRaises(ValueError):_scenario_sdk_size([{'result_1':UnknownInt(7)}])
            with self.assertRaises(ValueError):_scenario_sdk_size([{'result_1':SDKLong(2**63)}])
            with self.assertRaises(ValueError):_scenario_sdk_size([{'result_1':1.5}])

    def test_repinned_extra_database_row_refuses_before_runtime(self):
        import duckdb
        config,packs,sources,exports,receipt=self.fixture();database=config.prepared/'scenarios.duckdb';database.chmod(0o600)
        with duckdb.connect(str(database)) as connection:
            connection.execute('INSERT INTO carrier.archaeology_nodes SELECT * FROM carrier.archaeology_nodes LIMIT 1');connection.execute('CHECKPOINT')
        raw=database.read_bytes();database.chmod(0o444);receipt['database_bytes']=len(raw);receipt['database_sha256']=hashlib.sha256(raw).hexdigest()
        path=config.prepared/'receipt.json';path.chmod(0o600);path.write_text(json.dumps(receipt));path.chmod(0o444)
        with patch.object(PuppyLocalRuntime,'_docker') as docker:
            with self.assertRaises(ValueError):
                with open_puppy_scenario_local(config,pack_bytes=packs,sources=sources,exports=exports,preparation=receipt,native_map_profile=None):self.fail('No runtime acquisition')
        docker.assert_not_called()
