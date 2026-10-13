"""Public immutable-pair composition; native engine controls stay separately tested."""
from contextlib import contextmanager
from pathlib import Path
import hashlib,json,tempfile,unittest,tarfile
from unittest.mock import patch
from ashlar.graph_release import GraphRelease
from ashlar_host import puppy_export_cli as cli
from ashlar_host.puppy_finite_local import ImmutableExportPreparationPolicy

class Tests(unittest.TestCase):
    def configuration(self,root):
        evidence=Path(__file__).resolve().parents[1]/'docs/helix/04-build/evidence/truss15-core08-immutable-graphframes-20261013'
        raw=(evidence/'release.graph.json').read_bytes();release=GraphRelease(raw,hashlib.sha256(raw).hexdigest());custody=(evidence/'custody.json').read_bytes()
        (root/'release.json').write_bytes(release.payload);(root/'custody.json').write_bytes(custody)
        value={'profile':cli.PROFILE,'release_path':str(root/'release.json'),'custody_path':str(root/'custody.json'),
            'release_sha256':release.sha256,'custody_sha256':hashlib.sha256(custody).hexdigest(),'output':str(root/'out'),
            'docker':'/usr/bin/true','docker_host':'unix:///private/tmp/inert.sock','image':'sha256:'+'a'*64,
            'name':'ashlar-immutable-test','username':'inert','password':'inert','http_port':25480,'bolt_port':25481,'deadline_seconds':120}
        path=root/'configuration.json';path.write_text(json.dumps(value));return path,release

    def test_public_configuration_preparation_session_and_closing(self):
        for fail in (False,True):
            with self.subTest(close_failure=fail),tempfile.TemporaryDirectory()as directory:
                root=Path(directory).resolve();config,release=self.configuration(root);events=[]
                def prepare(export,out,*,policy,context):
                    digest=export.release.sha256;policy.admit_original_release(export.release,context);events.append('prepare');out.mkdir();(out/'db').write_bytes(b'inert');(out/'model.json').write_bytes(b'{}')
                    return {'release_sha256':digest}
                @contextmanager
                def runtime(configuration,*,export,receipt,failure_observer):
                    events.append('open');native=ImmutableExportPreparationPolicy(export);native.database=configuration.prepared/'db';native.schema_http=lambda *a:None;native.native_runtime=lambda *a:None
                    expected={sql:rows for _,sql,rows in cli.immutable_corpus(export.release)}
                    native.query=lambda language,sql,context:expected[sql]
                    try:yield native
                    finally:
                        events.append('close')
                        if fail:raise RuntimeError('owned cleanup failed')
                class Owner:
                    def __init__(self,adapter,policy):self.policy=policy
                    def refresh(self,release,*,context):
                        with self.policy.writer(context):self.policy.admit_original_release(release,context)
                        return release
                    def read(self,handle,*,context):
                        with self.policy.writer(context):self.policy.admit_original_release(handle,context)
                with patch.object(cli,'prepare_private_carrier',side_effect=prepare),patch.object(cli,'open_puppy_export_local',side_effect=runtime),patch.object(cli,'PuppyNativeReleaseAdapter')as adapter,patch.object(cli,'PuppyReleaseOwner',Owner):
                    if fail:
                        with self.assertRaisesRegex(RuntimeError,'cleanup failed'):cli.run_puppy_export_command(config)
                        self.assertFalse((root/'out/result.json').exists())
                    else:
                        result=cli.run_puppy_export_command(config);self.assertEqual(len(result['cases']),7);self.assertEqual(result['release_sha256'],release.sha256)
                self.assertEqual(json.loads(adapter.call_args.kwargs['prior_model']),{'catalog':[],'node':[],'edge':[]})
                self.assertEqual(events,['prepare','open','close'])

    def test_mixed_custody_refuses_before_preparation_or_runtime(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();config,_=self.configuration(root)
            docker=root/'docker';docker.symlink_to('/usr/bin/true');value=json.loads(config.read_text());value['docker']=str(docker);config.write_text(json.dumps(value))
            self.assertEqual(cli.parse_puppy_export_configuration(config.read_bytes())[2].docker,docker)
            missing={**value,'docker':str(root/'missing-docker')}
            with self.assertRaises(ValueError):cli.parse_puppy_export_configuration(json.dumps(missing).encode())
            (root/'custody.json').write_bytes(b'{}')
            with patch.object(cli,'prepare_private_carrier')as prepare,patch.object(cli,'open_puppy_export_local')as runtime:
                with self.assertRaises(ValueError):cli.run_puppy_export_command(config)
                prepare.assert_not_called();runtime.assert_not_called();self.assertFalse((root/'out').exists())

    def test_original_parallel_occurrence_oracle_keeps_isolate_and_equal_edges(self):
        release=GraphRelease(json.dumps({'nodes':[{'graph_id':'R1'},{'graph_id':'R2'}],
            'edges':[{'graph_id':k,'src':'R1','dst':'R1'}for k in ('E3','E4')]}).encode(),'a'*64)
        # Oracle reads original occurrence identity; no engine or source grant.
        with patch.object(cli,'PuppyReleaseHandle',return_value=type('Labels',(),{'node_label':'N','edge_label':'E'})()):
            corpus={name:(query,rows)for name,query,rows in cli.immutable_corpus(release)}
        self.assertEqual({(r['edge1'],r['edge2'])for r in corpus['two-hop'][1]},{('E3','E3'),('E3','E4'),('E4','E3'),('E4','E4')})
        self.assertEqual(corpus['two-hop-counts'][1],[{'key':'R1','paths':4,'targets':1},{'key':'R2','paths':0,'targets':0}])
        self.assertIn(' MATCH ',corpus['two-hop'][0]);self.assertEqual(corpus['degrees'][1],[{'key':'R1','degree':2},{'key':'R2','degree':0}])

    def test_actual_original_private_parallel_carrier_preparation(self):
        import duckdb
        from ashlar_host.local_graph_export import open_local_graph_export,LocalGraphExportSelection
        from ashlar_host.puppy_carrier import prepare_private_carrier,prepare_carrier
        archive=Path(__file__).resolve().parents[1]/'docs/helix/04-build/evidence/truss15-parallel-export-20261013/actual.tar.gz'
        with tempfile.TemporaryDirectory()as directory,tarfile.open(archive)as original:
            root=Path(directory).resolve();raw=original.extractfile('export.json-release.json').read();custody=original.extractfile('export.json-custody.json').read()
            (root/'release').write_bytes(raw);(root/'custody').write_bytes(custody)
            selection=LocalGraphExportSelection(root/'release',root/'custody',hashlib.sha256(raw).hexdigest(),hashlib.sha256(custody).hexdigest())
            with open_local_graph_export(selection)as export:
                policy=ImmutableExportPreparationPolicy(export)
                with self.assertRaises(PermissionError):prepare_private_carrier(export,root/'unheld',policy=policy,context=export.context)
                with policy.writer(export.context):
                    with self.assertRaises(ValueError):prepare_carrier(raw,selection.release_sha256,root/'strict',policy=policy,context=export.context)
                    receipt=prepare_private_carrier(export,root/'prepared',policy=policy,context=export.context)
                graph=json.loads(raw);db=duckdb.connect(str(root/'prepared'/receipt['database_name']),read_only=True)
                try:
                    for role in ('nodes','edges'):
                        rows=db.execute('SELECT * EXCLUDE (carrier_key,carrier_id) FROM carrier.'+role).fetchall();names=[d[0]for d in db.description]
                        self.assertEqual(sorted(json.dumps(dict(zip(names,row)),sort_keys=True)for row in rows),sorted(json.dumps(row,sort_keys=True)for row in graph[role]))
                finally:db.close()
                self.assertEqual(hashlib.sha256((root/'prepared'/receipt['database_name']).read_bytes()).hexdigest(),receipt['database_sha256'])
                self.assertEqual(len(graph['nodes']),2);self.assertEqual(len(graph['edges']),2)
