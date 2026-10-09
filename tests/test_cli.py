import base64
import json
from pathlib import Path
import subprocess
import sys
import unittest
from ashlar.source import jsonl_batches

ROOT = Path(__file__).resolve().parents[1]

class CLITests(unittest.TestCase):
    def invoke(self, raw):
        return subprocess.run([sys.executable, '-m', 'ashlar', 'inspect-source',
                               '--feed', 'fixture', '--epoch', 'epoch',
                               '--cursor-before', '18446744073709551616'],
                              input=raw, capture_output=True, cwd=ROOT)

    def test_original_batch_bytes_and_large_cursor_from_module_command(self):
        raw = (ROOT / 'examples/end-to-end/local-string-source.jsonl').read_bytes()
        result = self.invoke(raw)
        self.assertEqual(result.returncode, 0, result.stderr)
        values = [json.loads(line) for line in result.stdout.splitlines()]
        originals = tuple(jsonl_batches(raw.splitlines(keepends=True), feed='fixture', epoch='epoch',
                                       cursor_before='18446744073709551616'))
        self.assertEqual(len(values), len(originals))
        for value, original in zip(values, originals):
            self.assertEqual(value['cursor_after'], original.cursor_after)
            self.assertEqual(base64.b64decode(value['begin_base64']), original.begin)
            self.assertEqual(base64.b64decode(value['commit_base64']), original.commit)
            self.assertEqual([base64.b64decode(row['raw_base64']) for row in value['records']],
                             [row.raw for row in original.records])

    def test_incomplete_first_transaction_emits_no_batch(self):
        result = self.invoke(b'{"kind":"begin","batch_id":"incomplete"}\n')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, b'')

class CSVCLITests(unittest.TestCase):
    def invoke(self,raw,mapping='{"label":"23","caption":"24"}'):
        return subprocess.run([sys.executable,'-m','ashlar','inspect-csv','--feed','csv-example','--epoch','immutable-example-1','--source-system','local-example','--schema-revision','3','--type-id','17','--properties-json',mapping],input=raw,capture_output=True,cwd=ROOT)
    def test_user_input_custody_is_directly_recoverable_with_outer_checkpoint(self):
        from ashlar.csv_source import csv_batches
        from ashlar.staging import batch_from_row
        from ashlar.source_checkpoint import csv_checkpoint
        raw=(ROOT/'examples/end-to-end/string-source.csv').read_bytes()
        result=self.invoke(raw);self.assertEqual(result.returncode,0,result.stderr)
        values=[json.loads(line) for line in result.stdout.splitlines()]
        originals=tuple(csv_batches(raw.splitlines(keepends=True),feed='csv-example',epoch='immutable-example-1',source_system='local-example',schema_revision='3',type_id='17',properties={'label':'23','caption':'24'}))
        self.assertEqual(len(values),4)
        for value,batch in zip(values,originals):
            self.assertEqual(value['format'],'ashlar-csv-source-custody/0.1')
            self.assertEqual(batch_from_row(value['batch_row']),batch)
            self.assertEqual(value['source_checkpoint_json'],csv_checkpoint(batch))
    def test_invalid_or_duplicate_mapping_cannot_emit_custody(self):
        raw=(ROOT/'examples/end-to-end/string-source.csv').read_bytes()
        for mapping in ['{"label":23}','{"label":"23","label":"24"}','[]']:
            result=self.invoke(raw,mapping);self.assertNotEqual(result.returncode,0)
            self.assertEqual(result.stdout,b'')

class ConfiguredCLITests(unittest.TestCase):
    def test_configured_source_uses_installed_portable_components(self):
        result=subprocess.run([sys.executable,'-m','ashlar','inspect-configured-source',
            str(ROOT/'examples/end-to-end/configured-source.json')],capture_output=True,cwd=ROOT)
        self.assertEqual(result.returncode,0,result.stderr)
        value=json.loads(result.stdout)
        self.assertEqual(value['state'],'inspected-configured-source')
        self.assertEqual((value['batches'],value['records'],value['current_entities'],value['history_records'],value['tombstones']),(3,4,1,4,1))
        self.assertEqual(value['feed'],'configured-jsonl')
        self.assertTrue(value['exact_replay_unchanged'])
        self.assertNotIn('published',value)

class CommerceCLITests(unittest.TestCase):
    def test_explicit_original_inputs_preserve_candidate_and_refuse_changed_source(self):
        import tempfile,os
        from ashlar.commerce_source import build_transaction
        source=ROOT/'examples/domain-packs/commerce/upstream/ontology.json'
        graph=ROOT/'examples/domain-packs/commerce/upstream/graph/fixture.json'
        with tempfile.TemporaryDirectory() as directory:
            env=dict(os.environ);env['PYTHONPATH']=str(ROOT/'src')+os.pathsep+env.get('PYTHONPATH','')
            output=Path(directory)/'candidate'
            command=[sys.executable,'-m','ashlar','commerce-source','--ontology',str(source),
                '--graph',str(graph),'--output',str(output),'--source-system','cli-commerce']
            result=subprocess.run(command,capture_output=True,cwd=directory,env=env)
            self.assertEqual(result.returncode,0,result.stderr)
            batch,bindings=build_transaction(source.read_bytes(),graph.read_bytes(),source_system='cli-commerce')
            self.assertEqual((output/'source.jsonl').read_bytes(),batch.begin+b''.join(r.raw for r in batch.records)+batch.commit)
            self.assertEqual(json.loads((output/'bindings.json').read_bytes()),bindings)
            repeated=subprocess.run(command,capture_output=True,cwd=directory,env=env)
            self.assertNotEqual(repeated.returncode,0)
            changed=Path(directory)/'changed.json';changed.write_bytes(source.read_bytes()+b' ')
            refused=Path(directory)/'refused'
            command[command.index('--ontology')+1]=str(changed)
            command[command.index('--output')+1]=str(refused)
            result=subprocess.run(command,capture_output=True,cwd=directory,env=env)
            self.assertNotEqual(result.returncode,0)
            self.assertFalse(refused.exists())

class AdditionalPackCLITests(unittest.TestCase):
    def test_explicit_original_sources_run_outside_repository_and_refuse_changes(self):
        import tempfile,os,importlib
        for pack,count in [('archaeology',85),('ecology',94)]:
            with self.subTest(pack=pack),tempfile.TemporaryDirectory() as directory:
                module=importlib.import_module('ashlar.'+pack+'_source')
                source=ROOT/'examples/domain-packs'/pack/'upstream/ontology.json'
                graph=ROOT/'examples/domain-packs'/pack/'upstream/graph/fixture.json'
                output=Path(directory)/'candidate'
                env=dict(os.environ);env['PYTHONPATH']=str(ROOT/'src')
                command=[sys.executable,'-m','ashlar',pack+'-source','--ontology',str(source),'--graph',str(graph),'--output',str(output),'--source-system','cli-'+pack]
                result=subprocess.run(command,capture_output=True,cwd=directory,env=env)
                self.assertEqual(result.returncode,0,result.stderr)
                batch,binding=module.build_transaction(source.read_bytes(),graph.read_bytes(),source_system='cli-'+pack)
                self.assertEqual(len(batch.records),count)
                self.assertEqual((output/'source.jsonl').read_bytes(),batch.begin+b''.join(r.raw for r in batch.records)+batch.commit)
                self.assertEqual(json.loads((output/'bindings.json').read_bytes()),binding)
                self.assertNotEqual(subprocess.run(command,capture_output=True,cwd=directory,env=env).returncode,0)
                changed=Path(directory)/'changed.json';changed.write_bytes(source.read_bytes()+b' ')
                refused=Path(directory)/'refused';command[command.index('--ontology')+1]=str(changed);command[command.index('--output')+1]=str(refused)
                self.assertNotEqual(subprocess.run(command,capture_output=True,cwd=directory,env=env).returncode,0)
                self.assertFalse(refused.exists())
