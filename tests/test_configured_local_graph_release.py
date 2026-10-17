"""Actual core resolver/source/progress owners over inert native SQL only."""
from collections import Counter
import hashlib,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from ashlar.graph_release import validate_release_carrier,NODE_INTS,NODE_TEXT,EDGE_INTS,EDGE_TEXT
from ashlar.native import SQLResult
from ashlar_host.configured_local_graph_release import read_configured_local_graph_release
from ashlar_host.configured_graph_custody import build_configured_graph_custody
from ashlar_host.local_graph_export import LocalGraphExportSelection,open_local_graph_export
from ashlar_host.delta_custody import encoded
from test_configured_typed_graph_projection_oracle import graph_scope


def native_projection(scope):
    """Only supply native STRING projection metadata missing from shared fixture."""
    query=scope.transport.query
    def selected(sql,parameters):
        result=query(sql,parameters)
        if 'cast(unix_micros(published_at) AS STRING)'in sql:
            names=(NODE_INTS+NODE_TEXT if 'object_current'in sql else EDGE_INTS+EDGE_TEXT)+('published_at',)
            return SQLResult(result.rows,tuple((n,'STRING')for n in names))
        return result
    return patch.object(scope.transport,'query',side_effect=selected)


class Tests(unittest.TestCase):
    def test_complete_old_successor_graph_and_reusable_saved_pair(self):
        captured=[]
        for ordinal in (1,3):
            with graph_scope(ordinal)as scope,native_projection(scope):
                before=scope.transport.db.total_changes
                release=read_configured_local_graph_release(scope);graph=json.loads(release.payload)
                expected=scope.source_rows()
                for kind,role,key in (('node','object_current','nodes'),('edge','edge_current','edges')):
                    self.assertEqual(Counter(map(encoded,graph[key])),Counter(encoded(validate_release_carrier(dict(row),kind))for row in expected[role]))
                self.assertEqual(graph['snapshots'],{t:{'uuid':p[0],'version':p[1]}for t,p in scope.vector.targets.items()})
                self.assertEqual(scope.transport.db.total_changes,before)
                facts={'source':scope.source.metadata(scope.context),'binding_sha256':hashlib.sha256(scope.source.configuration.binding_source).hexdigest(),
                    'ordinal':ordinal,'request':scope.source.spec(ordinal,scope.context).request()}
            # Every real read/progress/source owner has closed before files exist.
            custody=build_configured_graph_custody(release,**facts)
            with tempfile.TemporaryDirectory()as tmp:
                root=Path(tmp).resolve();root.chmod(0o700)
                a=root/'release.graph.json';b=root/'custody.json';a.write_bytes(release.payload);b.write_bytes(custody)
                with open_local_graph_export(LocalGraphExportSelection(a,b,release.sha256,hashlib.sha256(custody).hexdigest()))as saved:
                    self.assertEqual(saved.release,release)
                bad=json.loads(custody);bad['snapshots'][next(iter(bad['snapshots']))]['version']+=1
                from ashlar_host.private_graph_custody import load_private_graph_release
                raw=(encoded(bad)+'\n').encode()
                with self.assertRaises(ValueError):load_private_graph_release(release.payload,release.sha256,raw,hashlib.sha256(raw).hexdigest())
            captured.append(graph)
        self.assertEqual([(len(g['nodes']),len(g['edges']))for g in captured],[(4,4),(3,1)])
        self.assertNotEqual(captured[0]['publication'],captured[1]['publication'])
        edges=captured[0]['edges'];self.assertTrue(any(e['src']==e['dst']for e in edges))
        self.assertGreater(len(edges),len({(e['src'],e['dst'])for e in edges}))

    def test_drift_mixed_vector_and_cancel_withhold(self):
        with graph_scope(1)as scope,native_projection(scope):
            original=scope.transport.query
            def drift(sql,parameters):
                result=original(sql,parameters)
                if 'cast(unix_micros(published_at) AS STRING)'in sql and 'edge_current'in sql:
                    return SQLResult(result.rows[:-1],result.columns)
                return result
            with patch.object(scope.transport,'query',side_effect=drift),self.assertRaises(ValueError):read_configured_local_graph_release(scope)
            from dataclasses import replace
            changed=dict(scope.vector.targets);table=next(iter(changed));changed[table]=(changed[table][0],changed[table][1]+1)
            original_vector=scope.vector
            object.__setattr__(scope,'vector',replace(original_vector,targets=changed))
            try:
                with self.assertRaises(ValueError):read_configured_local_graph_release(scope)
            finally:object.__setattr__(scope,'vector',original_vector)
            primary=KeyboardInterrupt()
            with patch.object(scope.transport,'query',side_effect=primary):
                with self.assertRaises(KeyboardInterrupt)as caught:read_configured_local_graph_release(scope)
                self.assertIs(caught.exception,primary)
        with self.assertRaises(ValueError):read_configured_local_graph_release(scope)

    def test_runtime_pair_waits_for_all_lower_owner_closings(self):
        # Runtime wiring control: native session/lease/admission are inert.
        # The separate tests above exercise the actual core read composition.
        from contextlib import contextmanager,ExitStack
        from types import SimpleNamespace
        from ashlar_host import configured_local_runtime as runtime
        from ashlar_host.configured_source_admission_config import ConfiguredSourceAdmissionConfig
        from ashlar_host.config import ProducerConfig
        for refuse in (False,True):
            with graph_scope(1)as scope,native_projection(scope):
                root=scope.transport.journal_path.parent;installation=root/'installation';installation.mkdir(mode=0o700)
                output=root/'export-output';producer=ProducerConfig(root/'producer',root/'bun',root/'git',30,1048576,4194304)
                admission=ConfiguredSourceAdmissionConfig(scope.source.configuration_file.path,producer,output/'admission',4194304)
                args=dict(installation=installation,output=output,jars=root/'jars',java_home=root/'java',admission=admission,
                    stream=scope.source.stream,materialization_clock_us=scope.descriptor.raw['recorded_at'],operation_capacity=None,
                    installation_id='inert-original',installation_sha256='a'*64)
                selected=runtime.ConfiguredLocalRuntimeConfig(**args);columns=runtime._columns()
                registry={'profile':'ashlar-configured-local-installation/0.1','scope':runtime._scope(selected,scope.source),
                    'columns':{r:[list(c)for c in cs]for r,cs in columns.items()},
                    'targets':[{'table':'local.configured.'+r,'path':str(installation/r),'uuid':scope.transport.targets[scope.tables[r]].uuid}for r in columns]}
                raw=(encoded(registry)+'\n').encode();(installation/'installation.json').write_bytes(raw)
                for role in columns:(installation/role).mkdir()
                from dataclasses import replace
                selected=replace(selected,installation_sha256=hashlib.sha256(raw).hexdigest());closed=[]
                @contextmanager
                def source_owner(*a,**kw):
                    try:yield scope.source
                    finally:closed.append('source')
                @contextmanager
                def read_owner(*a,**kw):yield scope
                def close_admission(*a):
                    closed.append('admission')
                    if refuse:raise ValueError('Closing native drift')
                class Current:
                    def __call__(self,*a):return None
                    closing=staticmethod(close_admission)
                spark=SimpleNamespace(sparkContext=SimpleNamespace(master='local[1]',setLogLevel=lambda _:None),
                    conf=SimpleNamespace(get=lambda key:'512m'if key=='spark.driver.memory'else'UTC'),stop=lambda:closed.append('spark'))
                lease=SimpleNamespace(renew=lambda:None,close=lambda:closed.append('lease'))
                progress=SimpleNamespace(retained_entries=lambda:(SimpleNamespace(descriptor=scope.descriptor),),position=lambda:'retained',completed_batches=lambda:3)
                original_write=runtime._write
                def write(path,raw):
                    if path.name in ('release.graph.json','custody.json'):
                        self.assertTrue({'admission','transport','spark','source','lease'}<=set(closed))
                    return original_write(path,raw)
                with ExitStack()as stack:
                    context_factory=type('InertContextFactory',(),{'__new__':lambda cls:scope.context,'__setattr__':object.__setattr__})
                    stack.enter_context(patch.object(runtime,'object',context_factory,create=True))
                    ports={'_runtime':lambda *_:[],'open_finite_spark':lambda *_:spark,'PrivateFiniteLease':lambda *a,**kw:lease,
                        'open_configured_local_source':source_owner,'ConfiguredLocalAdmission':lambda *a:Current(),'LocalDeltaTransport':lambda *a,**kw:scope.transport,'_write':write}
                    for name,value in ports.items():stack.enter_context(patch.object(runtime,name,value))
                    stack.enter_context(patch.object(scope.transport,'close',lambda:closed.append('transport')))
                    stack.enter_context(patch('ashlar_host.file_progress.JournaledJsonlProgress.open_existing',return_value=progress))
                    stack.enter_context(patch('ashlar_host.configured_local_read.open_configured_local_read',read_owner))
                    if refuse:
                        with self.assertRaises(ValueError):runtime.export_configured_local(selected,ordinal=1)
                        self.assertFalse((output/'release.graph.json').exists());self.assertFalse((output/'custody.json').exists())
                    else:
                        result=runtime.export_configured_local(selected,ordinal=1)
                        self.assertEqual((result['nodes'],result['edges']),(4,4));self.assertEqual(result['operation'],'export')
                        self.assertEqual(json.loads((output/'runtime-report.json').read_bytes()),result)
                        import sys
                        from ashlar_host.configured_local_cli import ConfiguredLocalInvocation,_result
                        invocation=ConfiguredLocalInvocation('export',1,Path(sys.executable),selected,300,1048576,262144)
                        self.assertEqual(_result(invocation,result),result)
                        for change in ({'nodes':True},{'release':str(root/'foreign')},{'custody_sha256':'a'*64}):
                            with self.assertRaises(ValueError):_result(invocation,dict(result,**change))
                self.assertTrue({'admission','transport','spark','source','lease'}<=set(closed))
