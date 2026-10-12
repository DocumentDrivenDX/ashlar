import base64,hashlib,json,unittest
from ashlar.apply import empty_state
from ashlar.truss_native_feed import native_feed_batch
from ashlar_host.graph_sql import graph_sql_plan,TABLES

class Tests(unittest.TestCase):
    def batch(self):
        encode=lambda v:json.dumps(v).encode()
        prior={'profile':'truss-caller-asserted-feed/0.1','installationId':'installation','sourceEpoch':'epoch','consumerId':'consumer','registrationId':'registration','nextXid':'0'}
        next={**prior,'nextXid':'43'}
        identity={'id':'7','typeId':'3','definitionPin':'type-pin'}
        image={'interfaceVersion':'truss-history-record/0.1.0','kind':'object','identity':identity,'recordVersion':'1','ownership':{'state':'rootless'},'properties':[{'propertyId':'9','definitionPin':'property-pin','value':'exact'}],'retained':[]}
        event={'interfaceVersion':'truss-history-event/0.2.0-proposal','operation':'create','sourceEpoch':'epoch','xid':'42','seq':'987','eventVersion':'1','eventCatalogRevision':'1','identity':identity,'after':image}
        raw=encode(event)
        wrapper={'interfaceVersion':'truss-journal-exact-bytes/0.2.0-proposal','component':'event','bytesBase64':base64.b64encode(raw).decode(),'bytesSha256':hashlib.sha256(raw).hexdigest()}
        payload={'interfaceVersion':'truss-native-feed/0.1','prior':prior,'next':next,'writerXid':'42','configuration':{},'revisions':[],'catalogPrerequisites':[],'sources':[],'reservations':[],'changes':[{'seq':987,'xid':'42','entity_kind':'o','entity_id':7,'entity_type':3,'ver':1,'new_value':wrapper}]}
        content=encode(payload)
        return native_feed_batch(content,sha256=hashlib.sha256(content).hexdigest(),prior=encode(prior),next=encode(next),feed='feed'),raw
    def test_native_create_plan_keeps_real_version_event_and_opaque_cursor(self):
        batch,raw=self.batch();after,steps=graph_sql_plan(empty_state(),batch,{key:'local.runtime.'+key for key in TABLES},materialized_at='2026-10-12T00:00:00+00:00',schema_policy=lambda change:None)
        self.assertEqual(next(iter(after.current.values())).version,1)
        rows=[row for step in steps if 'rows'in step['parameters']for row in json.loads(step['parameters']['rows'])]
        current=next(row for row in rows if 'props_json'in row)
        self.assertEqual(json.loads(current['source_cursor_json']),{'profile':'truss-native-feed/0.1','cursorBase64':batch.cursor_after})
        history=next(row for row in rows if 'raw_base64'in row);self.assertEqual(base64.b64decode(history['raw_base64']),raw)
        replay,replay_steps=graph_sql_plan(after,batch,{key:'local.runtime.'+key for key in TABLES},materialized_at='2026-10-12T00:00:00+00:00',schema_policy=lambda change:None)
        self.assertEqual(replay,after);self.assertEqual(replay_steps,[])

    def test_note_admission_requires_actual_report_type_and_required_fields(self):
        from ashlar_host.truss_note_admission import TrussNoteAdmission
        batch,_=self.batch();payload=json.loads(batch.begin)
        report={'installationId':'installation','revision':'1','types':[{'documentId':'ashlar-note','moduleId':'notes','elementId':'Note','typeId':'3'}],'properties':[{'typeId':'3','moduleId':'notes','elementId':'note-id','propertyId':'9'},{'typeId':'3','moduleId':'notes','elementId':'note-text','propertyId':'10'}]}
        from pathlib import Path
        original=Path('examples/truss-native/note.umf.json').read_bytes()
        report['acceptedInput']={'documents':[{'artifact':{'identity':'ashlar-note','sha256':hashlib.sha256(original).hexdigest(),'bytesBase64':base64.b64encode(original).decode()}}]}
        raw=json.dumps(report).encode();payload['catalogPrerequisites']=[{'revision':'1','reportBytesBase64':base64.b64encode(raw).decode(),'reportSha256':hashlib.sha256(raw).hexdigest()}]
        content=json.dumps(payload).encode()
        selected=native_feed_batch(content,sha256=hashlib.sha256(content).hexdigest(),prior=base64.b64decode(batch.cursor_before),next=base64.b64decode(batch.cursor_after),feed=batch.feed)
        # Complete event intentionally lacks required note-text: no semantic admission.
        with self.assertRaises(PermissionError):TrussNoteAdmission(selected)

        wrapper=payload['changes'][0]['new_value'];event=json.loads(base64.b64decode(wrapper['bytesBase64']))
        event['after']['properties'].append({'propertyId':'10','definitionPin':'text-pin','value':'complete text'})
        event_bytes=json.dumps(event).encode();wrapper.update(bytesBase64=base64.b64encode(event_bytes).decode(),bytesSha256=hashlib.sha256(event_bytes).hexdigest())
        content=json.dumps(payload).encode()
        complete=native_feed_batch(content,sha256=hashlib.sha256(content).hexdigest(),prior=base64.b64decode(batch.cursor_before),next=base64.b64decode(batch.cursor_after),feed=batch.feed)
        admitted=TrussNoteAdmission(complete)
        self.assertIsNone(admitted.admit(admitted.changes[0]));self.assertEqual(admitted.changes[0].state.key.type_id,3)
