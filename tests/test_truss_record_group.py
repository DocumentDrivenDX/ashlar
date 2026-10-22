# SPDX-License-Identifier: MIT OR Apache-2.0
"""Effect-free selections and public-port group controls; no native Truss claim."""
from dataclasses import dataclass,make_dataclass
from pathlib import Path
from types import ModuleType,SimpleNamespace
import json,tempfile,unittest
from unittest.mock import patch
from ashlar_host.truss_record_group import preflight,run_create_truss_record_group_command,release_report


class RecordGroupTests(unittest.TestCase):
    def fixture(self,root):
        (root/'bun').write_bytes(b'inert');(root/'model.json').write_text('{"umf":"0.8.0","id":"original","modules":[]}');(root/'origin.json').write_text('{"fixture":"original"}')
        value={'profile':'ashlar-truss-record-group/0.1','connection_uri':'private-ordinary','bun':str(root/'bun'),'document_path':str(root/'model.json'),'document_revision':'1','document_id':'original','source_version':'0.8.0','origin_path':str(root/'origin.json'),'request_key':'original-group','records':[{'alias':'supplier','module_id':'m','record_id':'supplier','values':{'id':{'kind':'string','text':'S1'}}},{'alias':'product','module_id':'m','record_id':'product','values':{'id':{'kind':'string','text':'P1'},'price':{'kind':'decimal','text':'12.75'}}}],'edges':[{'alias':'link','module_id':'m','relationship_id':'product-supplier','source':{'state':'alias','alias':'product'},'target':{'state':'alias','alias':'supplier'},'values':{},'order_key':{'state':'null'}}]}
        (root/'config.json').write_text(json.dumps(value));return value

    def test_preflight_original_version_immutable_selections_no_output(self):
        with tempfile.TemporaryDirectory()as temporary:
            root=Path(temporary).resolve();raw=self.fixture(root);config,output,_,model,_,_=preflight(root/'config.json',root/'result.json')
            self.assertEqual(config.source_version,'0.8.0');self.assertEqual(model,(root/'model.json').read_bytes())
            raw['records'][0]['values'].clear();self.assertEqual(config.records[0].values,(('id','string','S1'),));self.assertFalse(output.exists())

    def test_foreign_duplicate_alias_version_and_inexact_numeric_refuse_before_clients(self):
        with tempfile.TemporaryDirectory()as temporary:
            root=Path(temporary).resolve();original=self.fixture(root)
            for change in ('foreign','duplicate','version','numeric','extra'):
                value=json.loads(json.dumps(original))
                if change=='foreign':value['edges'][0]['target']['alias']='not-created'
                if change=='duplicate':value['edges'][0]['alias']='product'
                if change=='version':value['source_version']='0.7.0'
                if change=='numeric':value['records'][1]['values']['price']=12.75
                if change=='extra':value['effects']='trusted'
                (root/'config.json').write_text(json.dumps(value))
                with self.subTest(change=change), self.assertRaises(ValueError):preflight(root/'config.json',root/'result.json')
                self.assertFalse((root/'result.json').exists())

    def runtime(self,*,wrong_readback=False,missing_ref=False,primary=None,cleanup_error=None,refusal=None,first_committed_replay=False):
        contracts=ModuleType('truss.contracts');groups=ModuleType('truss.groups');truss=ModuleType('truss');psycopg=ModuleType('psycopg');calls={'apply':0,'commit':0,'close':0,'operations':None,'initial_replay':None}
        def dto(module,name,fields):
            cls=make_dataclass(name,fields,frozen=True);setattr(module,name,cls);return cls
        Text=dto(contracts,'TextValue',['kind','text']);Bool=dto(contracts,'BooleanValue',['value']);Null=dto(contracts,'NullValue',[]);dto(contracts,'MapValue',['entries']);dto(contracts,'MapEntry',['key','value'])
        dto(groups,'RootlessOwnership',[]);dto(groups,'CreateObject',['type_definition_pin','values','ownership','alias']);dto(groups,'CreateEdge',['relationship_definition_pin','source','target','values','order_key','alias']);dto(groups,'AuthoredValue',['name','value']);dto(groups,'AliasObjectReference',['alias']);dto(groups,'StoredObjectReference',['identity']);dto(groups,'NullOrderKey',[]);dto(groups,'TextOrderKey',['text'])
        Success=dto(groups,'GroupSuccess',['response']);Replay=dto(groups,'CommittedReceiptReplay',['semantic']);AppliedPending=dto(groups,'AppliedPending',['semantic']);Ok=dto(truss,'Ok',['value']);Error=dto(truss,'Error',['error']);ObjectFound=dto(truss,'ObjectFound',['object']);EdgeFound=dto(truss,'EdgeFound',['edge']);dto(truss,'CatalogDocument',['document_id','document_revision','content']);dto(truss,'CatalogRuntimeConfiguration',[])
        Identity=make_dataclass('Identity',['id','type_id','definition_pin'],frozen=True);Created=make_dataclass('Created',['identity','version','alias','operation','outcome']);Alias=make_dataclass('Alias',['alias','identity']);Semantic=make_dataclass('Semantic',['results','aliases']);Object=make_dataclass('Object',['document_id','module_id','record_id','id','identity','version','values','retained']);Edge=make_dataclass('Edge',['identity','version','source','target','order_key','retained']);objects={99:Object('original','m','supplier',99,Identity('99','1','supplier'),1,(('id','existing-supplier'),),())};edges={};semantic=None
        def plain(value):return value.text if isinstance(value,Text)and value.kind=='string'else value.value if isinstance(value,Bool)else None if isinstance(value,Null)else value
        class Assembly:
            def dispose(self):pass
            def get_type_reference_in_transaction(self,tx,**selection):return Ok(None if missing_ref else SimpleNamespace(definition_pin=selection['record_id']))
            def get_relationship_reference_in_transaction(self,tx,**selection):return Ok(SimpleNamespace(definition_pin=selection['relationship_id']))
            def group_context_in_transaction(self,tx):return Ok(SimpleNamespace(input=lambda operations,origin:operations,request=lambda key:key))
            def apply_in_transaction(self,tx,operations,request,**kw):
                nonlocal semantic
                calls['apply']+=1;calls['operations']=operations
                if primary:raise primary
                if refusal is not None:
                    calls['refusal'] = refusal(Error) if callable(refusal) else refusal
                    return calls['refusal']
                if first_committed_replay and calls['apply']==1:
                    calls['initial_replay'] = Success(Replay(None))
                    return Ok(calls['initial_replay'])
                if semantic is not None:return Ok(Success(Replay(semantic)))
                results=[];aliases=[];ids={}
                for index,op in enumerate(operations,1):
                    identity=Identity(str(index),str(index),op.type_definition_pin if hasattr(op,'type_definition_pin')else op.relationship_definition_pin);ids[op.alias]=identity;values=tuple((v.name,plain(v.value))for v in op.values)
                    if hasattr(op,'type_definition_pin'):
                        objects[index]=Object('original','m',op.type_definition_pin,index,identity,1,values,());kind='create_object'
                    else:
                        self_source=op.source.identity if hasattr(op.source,'identity')else ids[op.source.alias];self_target=op.target.identity if hasattr(op.target,'identity')else ids[op.target.alias];edges[index]=Edge(identity,1,self_source,self_target,None,values);kind='create_edge'
                    results.append(Created(identity,'1',op.alias,kind,'created'));aliases.append(Alias(op.alias,identity))
                semantic=Semantic(tuple(results),tuple(aliases));return Ok(Success(AppliedPending(semantic)))
            def get_object_in_transaction(self,tx,**selection):
                obj=objects[selection['object_id']]
                if wrong_readback:obj=Object('original','m','wrong',obj.id,obj.identity,obj.version,obj.values,obj.retained)
                return Ok(ObjectFound(obj))
            def get_edge_in_transaction(self,tx,**selection):return Ok(EdgeFound(edges[selection['edge_id']]))
        class Connection:
            def execute(self,*args):pass
            def commit(self):calls['commit']+=1
            def close(self):
                calls['close']+=1
                if cleanup_error:raise cleanup_error
        truss.create_reference_assembly=lambda config,host:Ok(Assembly());truss.adopt_psycopg_host=lambda connection:SimpleNamespace(adopt_transaction=lambda:Ok(object()),detach=lambda:True);psycopg.connect=lambda *args,**kw:Connection()
        return {'truss':truss,'truss.contracts':contracts,'truss.groups':groups,'psycopg':psycopg},calls

    def test_public_atomic_apply_readback_and_complete_original_replay(self):
        with tempfile.TemporaryDirectory()as temporary:
            root=Path(temporary).resolve();self.fixture(root);modules,calls=self.runtime()
            with patch.dict('sys.modules',modules):result=run_create_truss_record_group_command(root/'config.json',root/'result.json')
            self.assertEqual((calls['apply'],calls['commit'],calls['close']),(2,2,1))
            self.assertEqual([op.alias for op in calls['operations']],['supplier','product','link'])
            self.assertEqual(len(result['objects']),2);self.assertEqual(len(result['edges']),1)
            self.assertEqual(result['objects'][1]['values'][1][1],{'kind':'decimal','text':'12.75'})
            self.assertTrue((root/'result.json').is_file())


    def test_require_fresh_rejects_first_committed_replay_before_commit(self):
        from ashlar_host.truss_record_group import PublicTrussOutcomeError
        with tempfile.TemporaryDirectory()as temporary:
            root=Path(temporary).resolve();self.fixture(root)
            modules,calls=self.runtime(first_committed_replay=True)
            with patch.dict('sys.modules',modules),self.assertRaises(PublicTrussOutcomeError)as caught:
                run_create_truss_record_group_command(root/'config.json',root/'result.json',require_fresh=True)
            self.assertIs(caught.exception.outcome,calls['initial_replay'])
            self.assertEqual((calls['apply'],calls['commit'],calls['close']),(1,0,1))
            self.assertFalse((root/'result.json').exists())

    def test_missing_reference_and_wrong_actual_record_withhold_success(self):
        for missing in (True,False):
            with tempfile.TemporaryDirectory()as temporary:
                root=Path(temporary).resolve();self.fixture(root);modules,calls=self.runtime(missing_ref=missing,wrong_readback=not missing)
                with patch.dict('sys.modules',modules),self.assertRaises(RuntimeError):run_create_truss_record_group_command(root/'config.json',root/'result.json')
                self.assertFalse((root/'result.json').exists());self.assertEqual(calls['close'],1)
                if missing:self.assertEqual((calls['apply'],calls['commit']),(0,0))

    def test_cancellation_preserved_and_staged_write_failure_has_no_final_report(self):
        with tempfile.TemporaryDirectory()as temporary:
            root=Path(temporary).resolve();self.fixture(root);primary=KeyboardInterrupt();modules,calls=self.runtime(primary=primary)
            with patch.dict('sys.modules',modules),self.assertRaises(KeyboardInterrupt)as caught:run_create_truss_record_group_command(root/'config.json',root/'result.json')
            self.assertIs(caught.exception,primary);self.assertEqual((calls['commit'],calls['close']),(0,1))
            with patch('ashlar_host.truss_record_group.os.write',return_value=0),self.assertRaises(OSError):release_report(root/'result.json',b'complete')
            self.assertFalse((root/'result.json').exists());self.assertEqual(list(root.glob('.truss-group-*')),[])

    def test_typed_public_truss_refusal_is_preserved_without_stringifying_native_error(self):
        from ashlar_host.truss_record_group import PublicTrussOutcomeError
        with tempfile.TemporaryDirectory()as temporary:
            root=Path(temporary).resolve();self.fixture(root)
            failure=SimpleNamespace(code='cancelled',message='private native detail',retry_scope='none',sql_state='57014',catalog_code=None)
            modules,calls=self.runtime(refusal=lambda error_type:error_type(failure))
            with patch.dict('sys.modules',modules),self.assertRaises(PublicTrussOutcomeError)as caught:
                run_create_truss_record_group_command(root/'config.json',root/'result.json')
            self.assertIs(caught.exception.outcome,calls['refusal'])
            self.assertEqual(str(caught.exception),'Public Truss returned a typed non-success outcome')
            self.assertEqual((caught.exception.outcome.error.code,caught.exception.outcome.error.sql_state,
                              caught.exception.outcome.error.retry_scope),('cancelled','57014','none'))
            self.assertNotIn('private native detail',str(caught.exception))
            self.assertEqual((calls['apply'],calls['commit'],calls['close']),(1,0,1))
            self.assertFalse((root/'result.json').exists())

    def test_public_cli_dispatch_once_and_preserves_cancellation(self):
        import contextlib,io
        from ashlar import cli
        with tempfile.TemporaryDirectory()as temporary:
            root=Path(temporary).resolve();self.fixture(root)
            argv=['ashlar','create-truss-record-group','--configuration',str(root/'config.json'),'--output',str(root/'report.json')]
            with patch('sys.argv',argv),patch('ashlar_host.truss_record_group.run_create_truss_record_group_command',return_value={'scope':'inert'})as run,contextlib.redirect_stdout(io.StringIO()):cli.main()
            run.assert_called_once()
            self.assertFalse((root/'report.json').exists())
            primary=KeyboardInterrupt()
            with patch('sys.argv',argv),patch('ashlar_host.truss_record_group.run_create_truss_record_group_command',side_effect=primary),self.assertRaises(KeyboardInterrupt)as caught:cli.main()
            self.assertIs(caught.exception,primary)

    def test_stored_target_resolved_and_primary_survives_connection_close_failure(self):
        with tempfile.TemporaryDirectory()as temporary:
            root=Path(temporary).resolve();config=self.fixture(root)
            config['edges'][0]['target']={'state':'stored','document_id':'original','module_id':'m','record_id':'supplier','entity_id':99}
            (root/'config.json').write_text(json.dumps(config));modules,calls=self.runtime()
            with patch.dict('sys.modules',modules):result=run_create_truss_record_group_command(root/'config.json',root/'result.json')
            self.assertEqual(result['edges'][0]['target']['id'],'99')
        with tempfile.TemporaryDirectory()as temporary:
            root=Path(temporary).resolve();self.fixture(root);primary=KeyboardInterrupt();modules,calls=self.runtime(primary=primary,cleanup_error=OSError('private-uri'))
            with patch.dict('sys.modules',modules),self.assertRaises(KeyboardInterrupt)as caught:run_create_truss_record_group_command(root/'config.json',root/'result.json')
            self.assertIs(caught.exception,primary);self.assertTrue(primary.cleanup_failed);self.assertEqual(calls['close'],1)
            self.assertFalse((root/'result.json').exists())
