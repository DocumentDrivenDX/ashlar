"""Original local correspondence/locking controls; no native or UMF producer calls."""
import hashlib,json,os,sqlite3,tempfile,unittest
from pathlib import Path
from dataclasses import replace
from ashlar.source_config import load_jsonl_configuration
from ashlar.source import jsonl_batches
from ashlar.staging import batch_row
from ashlar.source_checkpoint import jsonl_checkpoint
from ashlar_host.managed_publication_selection import ManagedSelectionFile,ManagedPublicationSelectionSpec,open_managed_publication_selection,ROLES
ROOT=Path(__file__).resolve().parents[1]
def text(v):return json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False)
def digest(v):return hashlib.sha256(v.encode()if type(v)is str else v).hexdigest()

class Tests(unittest.TestCase):
    def fixture(self,root,ordinal=1):
        root=root.resolve()
        c=load_jsonl_configuration(ROOT/'examples/end-to-end/configured-source.json');namespace='test.original';stream='native-configured-jsonl-stream:'+namespace
        roles=(*ROLES,'publication_manifest','publication_attempt_phase','source_record','property_journal');installation={'state':'installed','namespace':namespace,'tables':{namespace+'.'+r:{'uuid':f'00000000-0000-0000-0000-{i:012d}'}for i,r in enumerate(roles,1)}}
        proof={'source_sha256':c.intake.source_sha256,'artifact_sha256':c.intake.artifact_sha256,'revision':c.intake.document_revision,'validator_revision':c.intake.validator_revision,'table':namespace+'.schema_intake','table_uuid':'00000000-0000-0000-0000-000000000099'}
        for name,value in [('installation',installation),('proof',proof)]:(root/name).write_text(text(value))
        batches=tuple(jsonl_batches(c.source.read_bytes().splitlines(keepends=True),feed=c.feed,epoch=c.epoch));batch=batches[0];row=batch_row(batch)
        request={'stream':stream,'batch_id':batch.batch_id,'predecessor':'empty-native-configured-jsonl-stream','schema_revisions_json':text({c.schema_alias:c.intake.document_revision}),'source_batch_json':row['batch_json'],'source_batch_digest':row['batch_digest'],'source_checkpoint_json':jsonl_checkpoint(batch)};request['request_digest']=digest(text(request));publication='configured-jsonl-stream-'+request['request_digest']
        sha=digest(c.source.read_bytes());manifest={'publication_id':publication,'profile_version':'ashlar-delta/0.3','table_versions_json':text({namespace+'.'+r:1 for r in ROLES}),'schema_revisions_json':request['schema_revisions_json'],'source_progress_json':text({c.feed:json.loads(jsonl_checkpoint(batch))}),'recorded_at':'1','validation_report_json':text({'complete':True,'request_digest':request['request_digest'],'source_sha256':sha,'source_groups':1,'intake_source_sha256':c.intake.source_sha256,'intake_artifact_sha256':c.intake.artifact_sha256})}
        artifact=text({'effects':{'intent_digest':request['request_digest']},'manifest':manifest});targets={namespace+'.'+r:{'uuid':installation['tables'][namespace+'.'+r]['uuid'],'version':1,'columns':[],'rows':[]}for r in ROLES};rq=text(request);effects=text({'intent_digest':request['request_digest']});tg=text(targets)
        db=sqlite3.connect(root/'journal');db.execute('PRAGMA journal_mode=WAL')
        statements=['stream_scope(id INTEGER,workload TEXT)','jsonl_consumer_scope(stream TEXT,feed TEXT,epoch TEXT,source_sha256 TEXT,config_json TEXT)','jsonl_consumer_progress(stream TEXT,feed TEXT,epoch TEXT,position INTEGER,original_json TEXT,digest TEXT)','snapshot_artifact(operation TEXT,request_json TEXT,effects_json TEXT,targets_json TEXT,artifact_text TEXT,artifact_digest TEXT,capture_digest TEXT)','publisher_effect_artifact(operation TEXT,request_json TEXT,steps_json TEXT,artifact_text TEXT,artifact_digest TEXT)','submission(operation TEXT,request TEXT,digest TEXT,handle TEXT,response TEXT)']
        for statement in statements:db.execute('CREATE TABLE '+statement)
        uuids={t:v['uuid']for t,v in installation['tables'].items()};workload={'namespace':namespace,'source_sha256':sha,'uuids':uuids,'intake_source_sha256':c.intake.source_sha256,'intake_table_uuid':proof['table_uuid'],'source_configuration_sha256':digest(c.original)}
        db.execute('INSERT INTO stream_scope VALUES (1,?)',(digest(text(workload)),));db.execute('INSERT INTO jsonl_consumer_scope VALUES (?,?,?,?,?)',(stream,c.feed,c.epoch,sha,text({'feed':c.feed,'epoch':c.epoch})))
        progress=text({'request':request,'descriptor':manifest});db.execute('INSERT INTO jsonl_consumer_progress VALUES (?,?,?,?,?,?)',(stream,c.feed,c.epoch,int(batch.cursor_after),progress,digest(progress)))
        db.execute('INSERT INTO publisher_effect_artifact VALUES (?,?,?,?,?)',('publisher-effects:'+namespace+':'+request['request_digest'],rq,'[]',artifact,digest(artifact)))
        db.execute('INSERT INTO snapshot_artifact VALUES (?,?,?,?,?,?,?)',('stream-artifact:'+request['request_digest'],rq,effects,tg,artifact,digest(artifact),digest(text([rq,effects,tg]))));db.commit()
        if ordinal==2:
            batch=batches[1];staged=batch_row(batch);request=dict(request,predecessor=publication,batch_id=batch.batch_id,source_batch_json=staged['batch_json'],source_batch_digest=staged['batch_digest'],source_checkpoint_json=jsonl_checkpoint(batch));request.pop('request_digest');request['request_digest']=digest(text(request));publication='configured-jsonl-stream-'+request['request_digest']
            manifest=dict(manifest,publication_id=publication,table_versions_json=text({namespace+'.'+r:2 for r in ROLES}),source_progress_json=text({c.feed:json.loads(jsonl_checkpoint(batch))}))
            report=json.loads(manifest['validation_report_json']);report.update(request_digest=request['request_digest'],source_groups=2);manifest['validation_report_json']=text(report)
            effects=text({'intent_digest':request['request_digest']});artifact=text({'effects':json.loads(effects),'manifest':manifest});rq=text(request);targets={t:dict(v,version=2)for t,v in targets.items()};tg=text(targets);second=text({'request':request,'descriptor':manifest})
            first=db.execute('SELECT * FROM jsonl_consumer_progress').fetchone();db.execute('DELETE FROM jsonl_consumer_progress')
            db.execute('INSERT INTO jsonl_consumer_progress VALUES (?,?,?,?,?,?)',(stream,c.feed,c.epoch,int(batch.cursor_after),second,digest(second)));db.execute('INSERT INTO jsonl_consumer_progress VALUES (?,?,?,?,?,?)',first)
            db.execute('INSERT INTO publisher_effect_artifact VALUES (?,?,?,?,?)',('publisher-effects:'+namespace+':'+request['request_digest'],rq,'[]',artifact,digest(artifact)))
            db.execute('INSERT INTO snapshot_artifact VALUES (?,?,?,?,?,?,?)',('stream-artifact:'+request['request_digest'],rq,effects,tg,artifact,digest(artifact),digest(text([rq,effects,tg]))));db.commit()
        from ashlar_host.managed_source_oracle import CLOCK_OPERATION,CLOCK_SQL
        clock_request=text({'authority':'original-actor-id','body':{'warehouse_id':'0123456789abcdef','statement':CLOCK_SQL,'parameters':[{'name':'workload','type':'STRING','value':digest(text(workload))}],'wait_timeout':'10s','on_wait_timeout':'CONTINUE','disposition':'INLINE','format':'JSON_ARRAY','row_limit':1000}})
        response={'statement_id':'original-clock-handle','status':{'state':'SUCCEEDED'},'manifest':{'format':'JSON_ARRAY','truncated':False,'total_row_count':1,'total_chunk_count':1,'schema':{'column_count':2,'columns':[{'position':0,'name':'now_us','type_text':'STRING'},{'position':1,'name':'workload','type_text':'STRING'}]},'chunks':[{'chunk_index':0,'row_offset':0,'row_count':1}]},'result':{'chunk_index':0,'row_offset':0,'row_count':1,'data_array':[['1700000000123456',digest(text(workload))]]}}
        db.execute('INSERT INTO submission VALUES (?,?,?,?,?)',(CLOCK_OPERATION,clock_request,digest(clock_request),'original-clock-handle',text(response)));db.commit()
        (root/'journal.native-csv-stream-lock').touch(mode=0o600)
        pin=lambda p:ManagedSelectionFile(p,digest(p.read_bytes()))
        return ManagedPublicationSelectionSpec(pin(root/'installation'),pin(root/'proof'),pin(c.path),root/'journal',publication,ordinal),db
    def test_wal_current_selection_and_independent_state_closing(self):
        with tempfile.TemporaryDirectory()as temp:
            spec,writer=self.fixture(Path(temp));before=writer.total_changes
            try:
                with open_managed_publication_selection(spec)as selected:
                    self.assertEqual(selected.descriptor.publication_id,spec.publication_id);self.assertTrue(selected.expected_state.current);self.assertEqual(len(selected.table_uuids),4);self.assertEqual(selected.original_materialization_clock_us,'1700000000123456');self.assertEqual(selected.original_actor_id,'original-actor-id');selected.renew()
                    with self.assertRaises(BlockingIOError):
                        with open_managed_publication_selection(spec):pass
                self.assertEqual(writer.total_changes,before)
            finally:writer.close()
    def test_two_completed_positions_are_selected_in_checkpoint_order(self):
        with tempfile.TemporaryDirectory()as temp:
            spec,writer=self.fixture(Path(temp),2)
            try:
                positions=[r[0]for r in writer.execute('SELECT position FROM jsonl_consumer_progress')]
                self.assertGreater(positions[0],positions[1])
                with open_managed_publication_selection(spec)as selected:
                    self.assertEqual(selected.descriptor.publication_id,spec.publication_id)
                    self.assertEqual(json.loads(selected.request_json)['source_checkpoint_json'],jsonl_checkpoint(selected.batches[1]));selected.renew()
            finally:writer.close()
    def test_correspondence_and_closing_failure_withhold_result(self):
        with tempfile.TemporaryDirectory()as temp:
            spec,writer=self.fixture(Path(temp));released=[]
            try:
                with self.assertRaises(PermissionError):
                    with open_managed_publication_selection(replace(spec,completed_ordinal=2)):pass
                with self.assertRaises(PermissionError):
                    with open_managed_publication_selection(spec):
                        writer.execute("UPDATE jsonl_consumer_progress SET digest=?",('0'*64,));writer.commit()
                    released.append(True)
                self.assertEqual(released,[])
            finally:writer.close()
    def test_unmatched_artifacts_and_mutable_policy_refuse(self):
        for table in ('publisher_effect_artifact','snapshot_artifact'):
            with self.subTest(table=table),tempfile.TemporaryDirectory()as temp:
                spec,writer=self.fixture(Path(temp))
                try:
                    original=writer.execute('SELECT * FROM '+table).fetchone();extra=('unfinished-extra',*original[1:]);writer.execute('INSERT INTO '+table+' VALUES ('+','.join('?'for _ in extra)+')',extra);writer.commit()
                    with self.assertRaises(PermissionError):
                        with open_managed_publication_selection(spec):pass
                finally:writer.close()
        with tempfile.TemporaryDirectory()as temp:
            spec,writer=self.fixture(Path(temp))
            try:
                with open_managed_publication_selection(spec)as selected:
                    with self.assertRaises(TypeError):selected.expected_state.current['foreign']=object()
                    with self.assertRaises(AttributeError):selected.expected_state=object()
                with self.assertRaises(PermissionError):
                    with open_managed_publication_selection(spec)as selected:selected.configuration.policy.source_system='foreign'
            finally:writer.close()
    def test_missing_lock_is_not_created(self):
        with tempfile.TemporaryDirectory()as temp:
            spec,writer=self.fixture(Path(temp));writer.close();lock=Path(str(spec.journal)+'.native-csv-stream-lock');lock.unlink()
            with self.assertRaises(FileNotFoundError):
                with open_managed_publication_selection(spec):pass
            self.assertFalse(lock.exists())

if __name__=='__main__':unittest.main()
