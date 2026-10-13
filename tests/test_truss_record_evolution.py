"""Small shared-record evolution authoring/preflight controls; no SDK execution."""
import copy,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from ashlar_host.truss_schema_evolution import run_evolve_truss_record_command,require_new_record_field
from ashlar_host.truss_note_evolution import run_evolve_truss_note_command

class Tests(unittest.TestCase):
    def test_cohort_revision_is_additive_and_bad_target_refuses_before_optional_sdk(self):
        root=Path(__file__).resolve().parents[1]/'examples/truss-native'
        old=json.loads((root/'cohort.umf.json').read_bytes());new=json.loads((root/'cohort-r2.umf.json').read_bytes())
        restored=copy.deepcopy(new);module=restored['modules'][0];record=next(element for element in module['elements']if element['id']=='B')
        self.assertEqual(record['members'][-1],{'module':'m','element':'B-tag'});record['members'].pop()
        added=module['elements'].pop();self.assertEqual(added['id'],'B-tag');self.assertEqual(added['nullability'],'absent-allowed')
        self.assertEqual(restored,old)
        inverse_old=json.loads((root.parents[1]/'src/ashlar_host/resources/cohort-inverse.umf.json').read_bytes())
        inverse_new=json.loads((root/'cohort-inverse-r2.umf.json').read_bytes())
        restored_inverse=copy.deepcopy(inverse_new)
        next(element for element in restored_inverse['modules'][0]['elements']if element['id']=='B')['members'].pop()
        restored_inverse['modules'][0]['elements'].pop()
        self.assertEqual(restored_inverse,inverse_old)
        require_new_record_field(json.dumps(inverse_new).encode(),json.dumps(inverse_old).encode(),module_id='m',record_id='B',field_name='B-tag')
        old_bytes=json.dumps(old).encode();new_bytes=json.dumps(new).encode()
        require_new_record_field(new_bytes,old_bytes,module_id='m',record_id='B',field_name='B-tag')
        with self.assertRaisesRegex(ValueError,'must declare the selected new field'):
            require_new_record_field(new_bytes,old_bytes,module_id='m',record_id='B',field_name='wrong-field')
        with self.assertRaisesRegex(ValueError,'already declared'):
            require_new_record_field(new_bytes,new_bytes,module_id='m',record_id='B',field_name='B-tag')
        with tempfile.TemporaryDirectory()as directory:
            path=Path(directory)/'configuration.json';value={key:'explicit'for key in ('connection_uri','bun','document_path','document_revision','origin_path','accept_request_key','update_request_key','document_id','module_id','record_id','field_name','field_value')}
            value.update(object_id=True,accept_request_key='accept',update_request_key='update');path.write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError,'positive object ID'):run_evolve_truss_record_command(path,Path(directory)/'result.json')
            self.assertFalse((Path(directory)/'result.json').exists())
    def test_note_wrapper_preserves_fixed_profile_and_original_config_without_tempfiles(self):
        with tempfile.TemporaryDirectory()as directory:
            path=Path(directory)/'configuration.json';config={key:'original-'+key for key in ('connection_uri','bun','document_path','document_revision','origin_path','accept_request_key','update_request_key','tag')};config['object_id']=3
            path.write_text(json.dumps(config));original=path.read_bytes()
            with patch('ashlar_host.truss_note_evolution.evolve_truss_record',return_value={'original':'result'})as run:
                self.assertEqual(run_evolve_truss_note_command(path,'/private/result.json'),{'original':'result'})
            selected=run.call_args.args[0];self.assertEqual(selected['document_id'],'ashlar-note');self.assertEqual(selected['module_id'],'notes');self.assertEqual(selected['record_id'],'Note');self.assertEqual(selected['field_name'],'note-tag');self.assertEqual(selected['field_value'],config['tag'])
            self.assertEqual(run.call_args.kwargs,{'required_original_fields':('note-id','note-text')});self.assertEqual(path.read_bytes(),original)
