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
    def test_original_element_selector_resolves_distinct_authored_name(self):
        old={'umf':'0.8.0','id':'commerce','modules':[{'id':'domain','elements':[{'id':'products','kind':'record','members':[]}]}],'extensions':{'opaque':{'preserved':True}}}
        new=copy.deepcopy(old);new['modules'][0]['elements'][0]['members'].append({'module':'domain','element':'products.display_label'})
        field={'id':'products.display_label','name':'display_label','kind':'field','scalarType':'string','cardinality':'one','nullability':'absent-allowed'};new['modules'][0]['elements'].append(field)
        original=json.dumps(new).encode();previous=json.dumps(old).encode()
        self.assertEqual(require_new_record_field(original,previous,module_id='domain',record_id='products',field_name='products.display_label'),'display_label')
        with self.assertRaisesRegex(ValueError,'must declare'):
            require_new_record_field(original,previous,module_id='domain',record_id='products',field_name='display_label')
        field['name']=''
        with self.assertRaisesRegex(ValueError,'declared Field name'):
            require_new_record_field(json.dumps(new).encode(),previous,module_id='domain',record_id='products',field_name='products.display_label')
        self.assertEqual(json.loads(original)['extensions'],old['extensions'])

    def test_public_update_uses_declared_name_and_complete_name_based_readback(self):
        from dataclasses import make_dataclass
        from types import ModuleType
        truss=ModuleType('truss');groups=ModuleType('truss.groups');contracts=ModuleType('truss.contracts')
        def dto(module,name,fields):
            cls=make_dataclass(name,fields,frozen=True);setattr(module,name,cls);return cls
        Ok=dto(truss,'Ok',['value']);Found=dto(truss,'ObjectFound',['object']);Accepted=dto(truss,'AcceptedDocument',['content','document_revision','catalog_revision']);Registration=dto(truss,'CatalogRegistration',['revision','registration_id','replayed','status','report_bytes']);dto(truss,'CatalogDocument',['document_id','document_revision','content']);dto(truss,'CatalogRuntimeConfiguration',[])
        Text=dto(contracts,'TextValue',['kind','text']);dto(contracts,'BooleanValue',['value']);dto(contracts,'MapValue',['entries']);dto(contracts,'MapEntry',['key','value']);dto(groups,'UpdateObject',['target','set','unset','expected_version']);dto(groups,'StoredObjectReference',['identity']);dto(groups,'AuthoredValue',['name','value']);Success=dto(groups,'GroupSuccess',['response']);Replay=dto(groups,'CommittedReceiptReplay',['semantic'])
        Identity=make_dataclass('Identity',['definition_pin']);Object=make_dataclass('Object',['id','type_id','version','values','retained','catalog_revision','identity','definition_pin']);Semantic=make_dataclass('Semantic',['results']);Response=make_dataclass('Response',['semantic']);calls=[];state={'accepted':False,'changed':False}
        old={'umf':'0.8.0','id':'commerce','modules':[{'id':'domain','elements':[{'id':'products','kind':'record','members':[]}]}]}
        revised=copy.deepcopy(old);revised['modules'][0]['elements'][0]['members'].append({'module':'domain','element':'products.display_label'});revised['modules'][0]['elements'].append({'id':'products.display_label','name':'display_label','kind':'field','scalarType':'string','cardinality':'one','nullability':'absent-allowed'})
        prior=json.dumps(old).encode();source=json.dumps(revised).encode();semantic=Semantic(('original-result',))
        class Assembly:
            def get_object_in_transaction(self,tx,**kwargs):
                values=(('unit_price',Text('decimal','13.75')),)+( (('display_label','Original product'),)if state['changed']else())
                pin='new-pin'if state['accepted']else'old-pin'
                return Ok(Found(Object(3,7,3 if state['changed']else 2,values,(),2 if state['accepted']else 1,Identity(pin),pin)))
            def accepted_document_in_transaction(self,tx,**kw):return Ok(Accepted(source if state['accepted']else prior,'2'if state['accepted']else'1',2 if state['accepted']else 1))
            def report_in_transaction(self,tx,revision):return Ok(SimpleNamespace(original_bytes=b'{}'))
            def prepare_catalog(self,documents,**kwargs):calls.append(('prepare',documents[0].content,kwargs['source_version']));return 'prepared'
            def accept_in_transaction(self,*args,**kwargs):state['accepted']=True;return Ok(Registration(2,'actual-registration',False,'accepted',b'{}'))
            def get_type_reference_in_transaction(self,*args,**kwargs):return Ok(SimpleNamespace(definition_pin='new-pin'))
            def group_context_in_transaction(self,tx):return Ok(SimpleNamespace(input=lambda ops,origin:ops,request=lambda key:key))
            def apply_in_transaction(self,tx,operations,*args,**kwargs):
                calls.append(('apply',operations));replayed=state['changed'];state['changed']=True
                return Ok(Success(Replay(semantic)if replayed else Response(semantic)))
            def dispose(self):pass
        class Connection:
            def __enter__(self):return self
            def __exit__(self,*args):return False
            def execute(self,*args):pass
            def commit(self):pass
        truss.create_reference_assembly=lambda *args:Ok(Assembly());truss.adopt_psycopg_host=lambda c:SimpleNamespace(adopt_transaction=lambda:Ok(object()),detach=lambda:True)
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();(root/'model').write_bytes(source);(root/'origin').write_bytes(b'{}');(root/'bun').write_bytes(b'inert')
            config={'connection_uri':'ordinary','bun':str(root/'bun'),'document_path':str(root/'model'),'document_revision':'2','origin_path':str(root/'origin'),'accept_request_key':'accept','update_request_key':'update','object_id':3,'document_id':'commerce','module_id':'domain','record_id':'products','field_name':'products.display_label','field_value':'Original product'};path=root/'config';path.write_text(json.dumps(config))
            with patch.dict(sys.modules,{'truss':truss,'truss.groups':groups,'truss.contracts':contracts,'psycopg':SimpleNamespace(connect=lambda *a,**kw:Connection())}),patch('ashlar_host.truss_schema_evolution.capture_catalog',side_effect=lambda a,t,r:json.dumps({'catalogRevision':r}).encode()):
                result=run_evolve_truss_record_command(path,root/'result')
            self.assertEqual(calls[0],('prepare',source,'0.8.0'));operations=calls[1][1];self.assertEqual(operations[0].set[0].name,'display_label');self.assertEqual(operations[0].expected_version,'2');self.assertEqual(calls[2][1],operations)
            self.assertEqual(dict(result['after']['values'])['display_label'],'Original product');self.assertNotIn('products.display_label',dict(result['after']['values']));self.assertEqual((root/'model').read_bytes(),source)

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
            new['modules'][0]['elements'].append({'id':'added','name':'added','kind':'field','scalarType':kind,'cardinality':'one','nullability':'absent-allowed'})
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
