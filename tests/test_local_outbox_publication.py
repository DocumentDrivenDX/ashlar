import json,unittest
from pathlib import Path
from run_local_outbox_publication import fixture,progress_union,validate_union,request_for,PrivatePolicy,ROOT,CLOCK
from fixture_oracle import fixture_columns,fixture_inventory
from ashlar.apply import empty_state
from ashlar.outbox import OutboxTransaction
from ashlar.source_checkpoint import outbox_checkpoint
from ashlar.whole_entity import changes_from_batch
from whole_graph_sql import graph_sql_plan
from local_delta_custody import encoded

class Tests(unittest.TestCase):
    def test_independent_sources_overlap_ids_without_collapsing_objects_edges_history(self):
        fixtures=fixture();batches=[fixtures[0]['batches'][0][1],fixtures[1]['batches'][0][1]]
        columns=fixture_columns(ROOT);expected,_=fixture_inventory(batches,columns,materialized_at=CLOCK)
        self.assertEqual(len(expected['object_current']),4);self.assertEqual(len(expected['edge_current']),2)
        self.assertEqual({r['id'] for r in expected['edge_current']},{'1'})
        self.assertEqual({r['source_system'] for r in expected['edge_current']},{f['source'] for f in fixtures})
        self.assertEqual(len(expected['whole_source_history']),6)
        # A updates/deletes must preserve B's equal numeric object/edge IDs.
        expected,_=fixture_inventory(batches+[fixtures[0]['batches'][1][1]],columns,materialized_at=CLOCK)
        self.assertEqual(len(expected['object_current']),3);self.assertEqual(len(expected['edge_current']),1)
        self.assertEqual(expected['edge_current'][0]['source_feed'],fixtures[1]['feed'])
        self.assertEqual(len(expected['tombstone']),2)
    def test_real_source_envelopes_closed_and_graph_sql_matches_declared_topology(self):
        fixtures=fixture();state=empty_state();allowed=tuple(c for f in fixtures for _,b in f['batches'] for c in changes_from_batch(b))
        def admit(change):
            if change not in allowed:raise PermissionError()
        tables={r:'local.pipeline.'+r for r in ('object_current','edge_current','tombstone','whole_source_history')}
        for f,i in [(fixtures[0],0),(fixtures[1],0),(fixtures[0],1),(fixtures[1],1)]:
            raw,batch=f['batches'][i]
            self.assertEqual(batch.cursor_before,'0');self.assertEqual(raw,batch.begin+b''.join(r.raw for r in batch.records)+batch.commit)
            state,steps=graph_sql_plan(state,batch,tables,materialized_at=CLOCK,schema_policy=admit)
            self.assertTrue(steps);self.assertTrue(all(set(s)=={'statement','parameters'} for s in steps))
        self.assertEqual(len(state.current),2);self.assertEqual(len(state.tombstones),4);self.assertEqual(len(state.deliveries),12)
    def test_interleaved_global_ancestry_is_distinct_from_source_progress_and_epochs(self):
        fixtures=fixture();progress={};previous='origin';revisions={}
        for n,(f,i) in enumerate([(fixtures[0],0),(fixtures[1],0),(fixtures[0],1),(fixtures[1],1)],1):
            raw,batch=f['batches'][i]
            import hashlib
            tx=OutboxTransaction('ashlar-postgresql-outbox/0.1',f['feed'],f['epoch'],str(i),str(i+1),hashlib.sha256(raw).hexdigest(),batch)
            revisions={**revisions,f['source']:'3'};request=request_for('global',tx,previous,revisions);checkpoint=json.loads(outbox_checkpoint(tx));new=progress_union(progress,checkpoint)
            manifest={'source_progress_json':encoded(new),'validation_report_json':encoded({'predecessor':previous,'request_digest':request['request_digest']})}
            validate_union(progress,request,manifest);progress=new;previous='publication-'+str(n)
        self.assertEqual({v['position'] for v in progress.values()},{'2'});self.assertEqual(len(progress),2);self.assertEqual(len(revisions),2)
        changed={**checkpoint,'epoch':'replacement'}
        with self.assertRaises(ValueError):progress_union(progress,changed)
        with self.assertRaises(ValueError):progress_union({},checkpoint)
        manifest['source_progress_json']=encoded({checkpoint['feed']:checkpoint})
        with self.assertRaises(ValueError):validate_union({fixtures[0]['feed']:progress[fixtures[0]['feed']]},request,manifest)
    def test_manifest_recovery_cannot_submit_without_original_native_intent(self):
        import sqlite3
        from types import SimpleNamespace
        from run_local_outbox_publication import ManifestPort
        from local_delta_custody import LocalDeltaError
        db=sqlite3.connect(':memory:');self.addCleanup(db.close)
        db.execute('CREATE TABLE local_operation(operation TEXT)')
        table='local.pipeline.manifest';transport=SimpleNamespace(db=db,targets={table:SimpleNamespace(table=table,uuid='original')})
        owner=SimpleNamespace(transport=transport,tables={'manifest':table},context=object())
        port=ManifestPort(owner,{'request_digest':'a'*64})
        port.store.commit=lambda *args,**kwargs:self.fail('Recovery submitted a replacement manifest')
        with self.assertRaises(LocalDeltaError):port.recover({},context=owner.context)
    def test_phase_and_manifest_mutations_bind_exact_original_request(self):
        from run_local_outbox_publication import PrivatePolicy
        context=object();policy=PrivatePolicy(context,());policy.initializing=False
        original={'request_digest':'a'*64,'batch_id':'original'};policy.active={'request':original,'steps':[],'manifest':{'publication_id':'original'}}
        intent={'profile':'ashlar-local-delta-operation/0.1','request_digest':'a'*64,'table':'local.pipeline.attempts','parameters':{'payload':encoded({'payload_json':encoded({'request':original})})}}
        policy.admit(intent,context)
        intent['parameters']['payload']=encoded({'payload_json':encoded({'request':{**original,'batch_id':'replacement'}})})
        with self.assertRaises(PermissionError):policy.admit(intent,context)
        intent['table']='local.pipeline.manifest';intent['parameters']={'row':encoded({'publication_id':'replacement'})}
        with self.assertRaises(PermissionError):policy.admit(intent,context)
    def test_zero_match_delete_specialization_is_source_qualified_and_retains_real_deletes(self):
        from run_local_outbox_publication import local_effect_plan
        fixtures=fixture();columns=fixture_columns(ROOT);tables={r:'local.pipeline.'+r for r in columns};state=empty_state()
        def admit(change):pass
        a=fixtures[0]['batches'][0][1];b=fixtures[1]['batches'][0][1]
        state,a_steps=graph_sql_plan(state,a,tables,materialized_at=CLOCK,schema_policy=admit)
        empty,_=fixture_inventory([],columns,materialized_at=CLOCK);selected,elisions=local_effect_plan(a_steps,empty,tables)
        self.assertEqual(len(elisions),2);self.assertTrue(all('INSERT INTO' in s['statement'] for s in selected))
        a_rows,_=fixture_inventory([a],columns,materialized_at=CLOCK)
        state,b_steps=graph_sql_plan(state,b,tables,materialized_at=CLOCK,schema_policy=admit)
        selected,elisions=local_effect_plan(b_steps,a_rows,tables)
        self.assertEqual(len(elisions),2)
        self.assertEqual({k['source_system'] for e in elisions for k in e['source_keys']},{fixtures[1]['source']})
        _,a_delete=graph_sql_plan(state,fixtures[0]['batches'][1][1],tables,materialized_at=CLOCK,schema_policy=admit)
        both,_=fixture_inventory([a,b],columns,materialized_at=CLOCK)
        selected,elisions=local_effect_plan(a_delete,both,tables)
        self.assertEqual(elisions,[]);self.assertEqual(selected,a_delete)
    def test_recovery_executor_dispatches_exact_original_child_and_never_mutation(self):
        from types import SimpleNamespace
        from run_local_outbox_publication import RecoveryExecutor
        from local_delta_custody import child_operation,LocalDeltaError
        calls=[]
        sql='MERGE INTO `local`.`pipeline`.`manifest` t USING original';params={'row':'original bytes'}
        expected=child_operation('manifest:'+'a'*64,sql,params)
        def recover(operation,statement,parameters,**kwargs):
            calls.append(operation)
            if operation!=expected:raise LocalDeltaError('Original operation absent')
            return 'original-receipt'
        transport=SimpleNamespace(recover=recover,mutation=lambda *a,**k:self.fail('Replacement mutation'),query=lambda *a:'read')
        executor=RecoveryExecutor(SimpleNamespace(transport=transport,operation='manifest:'+'a'*64,intent_digest='a'*64,context='held'))
        self.assertEqual(executor.query(sql,params),'original-receipt')
        with self.assertRaises(LocalDeltaError):executor.query(sql,{'row':'changed bytes'})
        self.assertEqual(executor.query('SELECT 1',{}),'read');self.assertEqual(len(calls),2)
    def test_manifest_descriptor_read_rechecks_original_uuid_after_select(self):
        from types import SimpleNamespace
        from run_local_outbox_publication import NativeDriver
        from local_delta_custody import LocalDeltaError
        table='local.pipeline.manifest';target=SimpleNamespace(uuid='original',table=table)
        native={'uuid':'original','checks':0};driver=NativeDriver.__new__(NativeDriver);driver.tables={'manifest':table}
        def detail(observed):
            native['checks']+=1
            if native['uuid']!=observed.uuid:raise LocalDeltaError('Original native manifest UUID replaced')
        def select(statement,parameters):
            self.assertEqual(parameters,{'id':'original-publication'})
            native['uuid']='replacement'
            return SimpleNamespace(rows=[{'publication_id':'original-publication'}])
        driver.transport=SimpleNamespace(targets={table:target},_detail=detail,query=select)
        with self.assertRaises(LocalDeltaError):driver.descriptors('original-publication')
        self.assertEqual(native['checks'],2)
    def test_explicit_source_port_required_and_metadata_cannot_change(self):
        from types import SimpleNamespace
        from run_local_outbox_publication import NativeDriver
        class Admission:
            facts={'profile':'explicit-test-only','qualification':'Finite test profile'}
            def metadata(self):return dict(self.facts)
            def admit(self,change):return None
        admission=Admission();driver=NativeDriver.__new__(NativeDriver);driver.source_admission=admission;driver.original_admission=encoded(admission.metadata());driver.allowed_changes=('original',)
        driver.schema_admit('original')
        with self.assertRaises(PermissionError):driver.schema_admit('commerce-replacement')
        admission.facts={**admission.facts,'profile':'replacement'}
        with self.assertRaises(PermissionError):driver.schema_admit('original')
        with self.assertRaises(TypeError):NativeDriver(None,None,None,None,None,None,None)
    def test_incomplete_or_reused_current_core_receipt_cannot_admit_other_source(self):
        import hashlib
        from run_local_outbox_publication import FixedStringAdmission,RECORD_CHECK_PIN
        changes=tuple(c for f in fixture() for _,b in f['batches'] for c in changes_from_batch(b))
        records=[{'deliveryId':c.delivery_id,'recordSha256':c.raw_digest,'result':{'validation':{'valid':True,'complete':True}}} for c in changes if c.state.key.kind=='object' and c.operation!='delete']
        receipt={'producerRevision':RECORD_CHECK_PIN,'sourceSha256':hashlib.sha256((ROOT/'examples/end-to-end/schema-v3.umf.json').read_bytes()).hexdigest(),'requestSha256':'a'*64,'records':records}
        admission=FixedStringAdmission(changes,receipt,receipt_bytes=encoded(receipt).encode());admission.admit(changes[0])
        wrong={**receipt,'records':records[:-1]}
        with self.assertRaises(PermissionError):FixedStringAdmission(changes,wrong,receipt_bytes=encoded(wrong).encode())
        from dataclasses import replace
        changed=replace(changes[0],state=replace(changes[0].state,key=replace(changes[0].state.key,type_id=1)))
        with self.assertRaises(PermissionError):FixedStringAdmission((changed,)+changes[1:],receipt,receipt_bytes=encoded(receipt).encode())
