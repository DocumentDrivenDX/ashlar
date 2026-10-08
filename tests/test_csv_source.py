import base64,json,sys,unittest
from pathlib import Path
from ashlar.csv_source import csv_batches
from ashlar.source import SourceError
from ashlar.whole_entity import changes_from_batch
from ashlar.apply import ApplyError
from ashlar.staging import batch_row,batch_from_row
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'))
from run_csv_example import run

class CSVSourceTests(unittest.TestCase):
    def batches(self,lines,**kw):
        return tuple(csv_batches(lines,feed='f',epoch='e',source_system='s',schema_revision='r',type_id='17',properties={'label':'23'},**kw))
    def test_full_example_replay_delete_and_unknown_custody(self):
        result=run()
        self.assertEqual((result['events'],result['objects'],result['tombstones']),(4,1,1))
        self.assertTrue(result['replay_unchanged']);self.assertFalse(result['complete_interpretation'])
    def test_exact_crlf_quote_unicode_empty_and_large_integer_text(self):
        lines=['id,entity_version,operation,label,future\r\n'.encode(),'9223372036854775807,1,create,"雪, ""quote""",18446744073709551615\r\n'.encode()]
        batch=self.batches(lines)[0];restored=batch_from_row(batch_row(batch))
        self.assertEqual(batch,restored)
        custody=json.loads(restored.records[0].delivery_id)
        self.assertEqual(base64.b64decode(custody['header_base64']),lines[0])
        self.assertEqual(base64.b64decode(custody['row_base64']),lines[1])
        state=changes_from_batch(restored)[0].state
        self.assertEqual(state.key.id,9223372036854775807)
        self.assertEqual(json.loads(state.props_json),{'23':'雪, "quote"'})
        self.assertEqual(json.loads(state.retained_json)['unmapped_columns']['future'],'18446744073709551615')
    def test_invalid_csv_controls_or_bounds_refuse(self):
        header=b'id,entity_version,operation,label\n'
        for lines in [[],[header,b'1,1,create,x'],[header,b'1,1,create,"a\nb"\n'],[header,b'1,1,create\n'],[header,b'01,1,create,x\n'],[header,b'1,-1,create,x\n'],[header,b'1,1,patch,x\n'],[b'id,id,entity_version,operation,label\n'],[header,b'1,1,create,\xff\n']]:
            with self.assertRaises((SourceError,ApplyError)):self.batches(lines)
        with self.assertRaises(SourceError):self.batches([header,b'1,1,create,x\n',b'2,1,create,y\n'],max_records=1)
        with self.assertRaises(SourceError):self.batches([header],max_line_bytes=3)
    def test_incremental_consumption_and_mapping_snapshot(self):
        seen=[]
        def lines():
            for raw in [b'id,entity_version,operation,label\n',b'1,1,create,x\n',b'2,1,create,y\n']:
                seen.append(raw);yield raw
        iterator=csv_batches(lines(),feed='f',epoch='e',source_system='s',schema_revision='r',type_id='17',properties={'label':'23'})
        self.assertEqual(next(iterator).batch_id,'csv-row-1');self.assertEqual(len(seen),2)
        self.assertEqual(next(iterator).batch_id,'csv-row-2')
        with self.assertRaises(StopIteration):next(iterator)
if __name__=='__main__':unittest.main()

class CSVCorrespondenceTests(unittest.TestCase):
    def validate(self,batch,**delta):
        from ashlar.csv_source import validate_csv_batch
        config=dict(feed='f',epoch='e',source_system='s',schema_revision='r',type_id='17',properties={'label':'23'})
        config.update(delta);return validate_csv_batch(batch,**config)
    def batches(self):return tuple(csv_batches([b'id,entity_version,operation,label,future\n',b'1,1,create,first,opaque\n',b'1,2,replace,second,opaque\n'],feed='f',epoch='e',source_system='s',schema_revision='r',type_id='17',properties={'label':'23'}))
    def test_all_ordinals_revalidate_without_reconstructing_preceding_rows(self):
        for batch in self.batches():self.assertIsNone(self.validate(batch))
    def test_valid_but_substituted_event_with_rehashed_commit_refuses(self):
        from ashlar.source import jsonl_batches,records_digest
        batch=self.batches()[1];event=json.loads(batch.records[0].raw)
        event['props_json']='{"23":"substituted"}'
        record=(json.dumps(event,separators=(',',':'))+'\n').encode()
        commit=json.loads(batch.commit);commit['records_sha256']=records_digest([record])
        forged=next(jsonl_batches([batch.begin,record,(json.dumps(commit,separators=(',',':'))+'\n').encode()],feed='f',epoch='e'))
        with self.assertRaises(SourceError):self.validate(forged)
    def test_untrusted_source_mapping_schema_identity_refuse(self):
        batch=self.batches()[0]
        for delta in [dict(feed='other'),dict(epoch='other'),dict(source_system='other'),dict(schema_revision='other'),dict(type_id='18'),dict(properties={'future':'23'})]:
            with self.assertRaises(SourceError):self.validate(batch,**delta)
