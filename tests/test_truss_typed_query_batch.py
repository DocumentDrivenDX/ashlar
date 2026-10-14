import json,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from contextlib import contextmanager
from unittest.mock import patch
import ashlar_host.truss_typed_query_batch as command

class Tests(unittest.TestCase):
    def fixture(self,root):
        executable=root/'weft';executable.write_text('original');index=root/'index.json';index.write_text('original');installation=root/'installed';installation.mkdir()
        queries=[]
        for name,family in (('fields','scalar'),('groups','numeric-grouped-count'),('paths','two-hop-count'),('joins','scalar-join')):
            q={'name':name,'family':family,'sql':'SELECT original '+name,'revision':'1','document_id':'typed-related-record','maximum_rows':2}
            if family!='two-hop-count':q['physical_revisions']=['1']
            queries.append(q)
        return {'profile':'ashlar-truss-typed-query-batch/0.1','compilers':{'compatible':{'executable':str(executable)},'indexed':{'index':str(index),'installation':str(installation)}},'queries':queries}
    def test_one_owner_original_protocol_and_no_partial_report(self):
        for outcome in ('success','manifest-drift','closing-failure'):
            with self.subTest(outcome=outcome),tempfile.TemporaryDirectory()as directory:
                root=Path(directory).resolve();value=self.fixture(root);batch=root/'batch.json';batch.write_text(json.dumps(value));output=root/'output.json'
                driver=SimpleNamespace(context=object(),source_sessions=SimpleNamespace(batch=lambda:SimpleNamespace(feed='feed')),policy=SimpleNamespace(active={'manifest':{'publication_id':'pub5','vector':[1,2,3,4]}}))
                @contextmanager
                def writer(*args):yield
                driver.writer=writer;seen=[]
                def executor(driver,compiler,sql,**kwargs):
                    seen.append((compiler,sql,kwargs));answer={'publication_id':'pub5','rows':[{'original':'cell'}],'request_bytes':b'{"request":"original"}\n','response_bytes':b'{"response":"original"}\n'}
                    if sql.endswith('paths'):answer['recompiled_bytes']=b'{"response":"original"}\n'
                    if outcome=='manifest-drift':driver.policy.active['manifest']['vector'][0]=99
                    return answer
                def owner(configuration,destination,operation,*,report_writer):
                    result=operation(driver,{'request_digest':'digest'});self.assertFalse(output.exists())
                    if outcome=='closing-failure':raise RuntimeError('source close failed')
                    return report_writer(result,destination)
                with patch.object(command,'run_selected_truss_command',side_effect=owner)as selected,patch.object(command,'restore_applied_truss_publication')as restore,patch.object(command,'execute_truss_scalar_query',side_effect=executor),patch.object(command,'execute_truss_numeric_grouped_count_query',side_effect=executor),patch.object(command,'execute_truss_two_hop_count_query',side_effect=executor),patch.object(command,'execute_truss_scalar_join_query',side_effect=executor)as join:
                    if outcome=='success':
                        result=command.run_query_truss_typed_batch_command('/original',output,batch=batch)
                        self.assertEqual(len(seen),4);self.assertIsInstance(seen[0][0],command.CompatibleCompiler);self.assertIsInstance(seen[2][0],command.ImplicitArithmeticDistributionPaths)
                        join.assert_called_once();self.assertIs(seen[3][0],seen[0][0]);self.assertEqual(seen[3][2]['physical_revisions'],('1',))
                        self.assertEqual(result['queries'][3]['family'],'scalar-join');self.assertEqual(result['queries'][3]['request_original_json'],'{"request":"original"}\n')
                        self.assertEqual(seen[0][2]['physical_revisions'],('1',));self.assertNotIn('physical_revisions',seen[2][2])
                        self.assertEqual(result['queries'][2]['recompiled_original_json'],'{"response":"original"}\n');self.assertNotIn('recompiled_original_json',result['queries'][0])
                        self.assertEqual(result['protocol_bytes'],sum(len(b'{"request":"original"}\n')+len(b'{"response":"original"}\n')for _ in range(4))+len(b'{"response":"original"}\n'))
                        self.assertEqual(output.stat().st_mode&0o777,0o600)
                    else:
                        with self.assertRaises((ValueError,RuntimeError)):command.run_query_truss_typed_batch_command('/original',output,batch=batch)
                        self.assertFalse(output.exists())
                        if outcome=='manifest-drift':self.assertEqual(len(seen),1)
                    restore.assert_called_once();selected.assert_called_once()
    def test_closed_families_transports_and_path_refusal_before_effects(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();value=self.fixture(root)
            compatible_only=dict(value,queries=value['queries'][:2],compilers={'compatible':value['compilers']['compatible']})
            parsed=command.parse_truss_typed_query_batch(json.dumps(compatible_only).encode())
            self.assertIsInstance(parsed,command.TrussTypedQueryBatch);self.assertIsInstance(parsed.queries,tuple)
            self.assertEqual(parsed.queries[0].physical_revisions,('1',));self.assertIsNone(parsed.indexed)
            from dataclasses import FrozenInstanceError
            with self.assertRaises(FrozenInstanceError):parsed.queries[0].sql='changed'
            with self.assertRaises(FrozenInstanceError):parsed.compatible.executable='/changed'
            with self.assertRaises(FrozenInstanceError):parsed.queries=()

            indexed_only=dict(value,queries=value['queries'][2:3],compilers={'indexed':value['compilers']['indexed']})
            command.parse_truss_typed_query_batch(json.dumps(indexed_only).encode())
            for invalid in (dict(value,queries=[dict(value['queries'][2],physical_revisions=['1'])]),dict(value,queries=[dict(value['queries'][0],family='invented')]),dict(compatible_only,compilers=value['compilers'])):
                with self.assertRaises(ValueError):command.parse_truss_typed_query_batch(json.dumps(invalid).encode())
            batch=root/'batch.json';batch.write_text(json.dumps(value))
            with patch.object(command,'run_selected_truss_command')as owner:
                with self.assertRaises(ValueError):command.run_query_truss_typed_batch_command('/original',root/'installed'/'output.json',batch=batch)
                owner.assert_not_called()
    def test_cli_dispatch(self):
        from ashlar.cli import main
        with patch.object(command,'run_query_truss_typed_batch_command',return_value={'publication_id':'pub5','queries':[{}]})as run,patch('sys.argv',['ashlar','query-truss-typed-batch','--configuration','/reader','--output','/result','--batch','/batch']):
            main();run.assert_called_once_with('/reader','/result',batch='/batch')
