"""Small shared-record evolution authoring/preflight controls; no SDK execution."""
import copy,json,tempfile,unittest,sys
from dataclasses import dataclass
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch
from ashlar_host.truss_schema_evolution import run_evolve_truss_record_command,require_new_record_field,selected_field_value,public_field_values
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
    def test_exact_typed_new_member_and_public_values(self):
        @dataclass
        class Text:kind:str;text:str
        @dataclass
        class Boolean:value:bool
        old={'modules':[{'id':'m','elements':[{'id':'Record','kind':'record','members':[]}]}]}
        for tag in ({'kind':'integer','text':'9007199254740993'},{'kind':'decimal','text':'1.2300'},{'kind':'boolean','value':True},{'kind':'string','text':'legacy'}):
            kind,body=selected_field_value(tag);new=copy.deepcopy(old)
            new['modules'][0]['elements'][0]['members'].append({'module':'m','element':'added'})
            new['modules'][0]['elements'].append({'id':'added','kind':'field','scalarType':kind,'cardinality':'one','nullability':'absent-allowed'})
            require_new_record_field(json.dumps(new).encode(),json.dumps(old).encode(),module_id='m',record_id='Record',field_name='added',scalar_type=kind)
            with patch.dict(sys.modules,{'truss.contracts':SimpleNamespace(TextValue=Text,BooleanValue=Boolean)}):authored,observed=public_field_values(kind,body)
            if kind in ('integer','decimal'):self.assertEqual(authored,Text(kind,body));self.assertIs(observed,authored)
            elif kind=='boolean':self.assertEqual(authored,Boolean(True));self.assertIs(observed,True)
            else:self.assertEqual(authored,Text('string','legacy'));self.assertEqual(observed,'legacy')
            with self.assertRaisesRegex(ValueError,'match exact scalar'):
                require_new_record_field(json.dumps(new).encode(),json.dumps(old).encode(),module_id='m',record_id='Record',field_name='added',scalar_type='boolean'if kind!='boolean'else'integer')
        self.assertEqual(selected_field_value('legacy'),('string','legacy'))
        for invalid in ({'kind':'null'},{'kind':'integer','text':'1.0'},{'kind':'decimal','text':'NaN'},{'kind':'boolean','value':1}):
            with self.assertRaises(ValueError):selected_field_value(invalid)
