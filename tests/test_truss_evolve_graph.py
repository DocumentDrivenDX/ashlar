# SPDX-License-Identifier: MIT OR Apache-2.0
import json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import ashlar_host.truss_evolve_graph as m

class Tests(unittest.TestCase):
    def inputs(self,root):
        source={'connection_uri':'same','bun':str(root/'bun'),'document_path':str(root/'model'),'origin_path':str(root/'origin')}
        reader={'connection_uri':'same','publication_directory':str(root/'publication'),'jars':[str(root/'jar')],'feed':'feed','source_page_path':str(root/'page'),'source_page_sha256':'a'*64,'request_digest':'b'*64,'destination_id':'selected-destination'}
        for name,value in (('source.json',source),('reader.json',reader)):(root/name).write_text(json.dumps(value))
        return source,reader
    def test_parser_preserves_original_origin_bytes(self):
        from ashlar_host.truss_schema_evolution import parse_truss_record_evolution_configuration
        with tempfile.TemporaryDirectory()as d:
            root=Path(d).resolve();(root/'bun').write_text('selected')
            (root/'model').write_text('{"id":"cohort","umf":"0.7.0"}')
            original=b'{  "operator" : "selected" }\n';(root/'origin').write_bytes(original)
            config={'connection_uri':'same','bun':str(root/'bun'),'document_path':str(root/'model'),'document_revision':'r2','origin_path':str(root/'origin'),'accept_request_key':'accept','update_request_key':'update','object_id':2,'document_id':'cohort','module_id':'m','record_id':'B','field_name':'B-tag','field_value':'reviewed'}
            self.assertEqual(parse_truss_record_evolution_configuration(config,root/'result.json')[4],original)
    def test_happy_preserves_selection_and_closes_components_before_report(self):
        with tempfile.TemporaryDirectory()as d:
            root=Path(d).resolve();source,reader=self.inputs(root);out=root/'output';events=[]
            def evolve(configuration,destination):events.append('source');destination.write_text('{"original":"catalog"}');return {}
            def continuation(configuration,destination,**kwargs):
                self.assertEqual(kwargs,{'maximum_pages':2,'wait_seconds':0,'maximum_page_bytes':2097152})
                captured=json.loads(configuration.read_text());self.assertEqual(captured['destination_id'],'selected-destination');self.assertEqual(captured['request_digest'],reader['request_digest']);self.assertEqual(len(captured['archived_acceptance_files']),1)
                self.assertFalse((out/'result.json').exists());events.append('continued');return {'publications':[{'manifest':{'publication_id':'one'}},{'manifest':{'publication_id':'two'}}]}
            def configured(*args):events.append('configured');args[2].write_text('{}')
            with patch.object(m,'parse_truss_record_evolution_configuration'),patch.object(m,'parse_resume_truss_configuration',return_value=(reader,root/'publication',b'',(root/'jar',),())),patch.object(m,'evolve_truss_record',side_effect=evolve),patch.object(m,'run_continue_truss_graph_command',side_effect=continuation),patch.object(m,'run_configure_truss_continuation_command',side_effect=configured):
                result=m.run_evolve_truss_graph_command(root/'source.json',root/'reader.json',out)
            self.assertEqual(events,['source','continued','configured']);self.assertEqual(result['publication_id'],'two');self.assertEqual((out/'result.json').stat().st_mode&0o777,0o600)
    def test_foreign_source_refuses_before_effect_and_continuation_failure_retains_original(self):
        with tempfile.TemporaryDirectory()as d:
            root=Path(d).resolve();source,reader=self.inputs(root);out=root/'output';reader['connection_uri']='foreign'
            with patch.object(m,'parse_truss_record_evolution_configuration'),patch.object(m,'parse_resume_truss_configuration',return_value=(reader,root/'publication',b'',(),())),patch.object(m,'evolve_truss_record')as evolve:
                with self.assertRaises(ValueError):m.run_evolve_truss_graph_command(root/'source.json',root/'reader.json',out)
                evolve.assert_not_called();self.assertFalse(out.exists())
            reader['connection_uri']='same'
            def evolve(configuration,destination):destination.write_text('{"original":"catalog"}')
            failure=RuntimeError('original continuation failure')
            with patch.object(m,'parse_truss_record_evolution_configuration'),patch.object(m,'parse_resume_truss_configuration',return_value=(reader,root/'publication',b'',(),())),patch.object(m,'evolve_truss_record',side_effect=evolve),patch.object(m,'run_continue_truss_graph_command',side_effect=failure):
                with self.assertRaises(RuntimeError)as raised:m.run_evolve_truss_graph_command(root/'source.json',root/'reader.json',out)
            self.assertIs(raised.exception,failure);self.assertTrue((out/'evolution.json').exists());self.assertFalse((out/'result.json').exists())
