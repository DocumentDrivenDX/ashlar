import base64,hashlib,json,unittest
from ashlar.apply import empty_state
from ashlar.truss_native_feed import native_feed_batch
from ashlar_host.graph_sql import graph_sql_plan,TABLES
from ashlar_host.schema_rows import fixture_columns
from ashlar_host.resources import RESOURCE_ROOT
from ashlar_host.truss_note_publication import note_expected_rows

class Tests(unittest.TestCase):
    def test_independent_note_rows_match_create_plan_and_exact_clock(self):
        encode=lambda value:json.dumps(value).encode()
        prior={'profile':'truss-caller-asserted-feed/0.1','installationId':'installation','sourceEpoch':'epoch','consumerId':'consumer','registrationId':'registration','nextXid':'0'};following={**prior,'nextXid':'43'}
        image={'interfaceVersion':'truss-history-record/0.1.0','kind':'object','identity':{'typeId':'3','id':'7'},'recordVersion':'1','ownership':{'state':'rootless'},'properties':[{'propertyId':'9','definitionPin':'pin','value':{'kind':'string','text':'exact'}}],'retained':[]}
        event={'interfaceVersion':'truss-history-event/0.2.0-proposal','sourceEpoch':'epoch','xid':'42','seq':'987','eventVersion':'1','eventCatalogRevision':'1','operation':'create','after':image};raw=encode(event)
        row={'seq':987,'xid':'42','entity_kind':'o','entity_id':7,'entity_type':3,'ver':1,'new_value':{'interfaceVersion':'truss-journal-exact-bytes/0.2.0-proposal','component':'event','bytesBase64':base64.b64encode(raw).decode(),'bytesSha256':hashlib.sha256(raw).hexdigest()}}
        content=encode({'interfaceVersion':'truss-native-feed/0.1','prior':prior,'next':following,'writerXid':'42','changes':[row],'configuration':{},'catalogPrerequisites':[],'revisions':[],'sources':[],'reservations':[]})
        batch=native_feed_batch(content,sha256=hashlib.sha256(content).hexdigest(),prior=encode(prior),next=encode(following),feed='feed')
        from ashlar.truss_native_feed import scalar_object_changes
        extracted=scalar_object_changes(content,sha256=hashlib.sha256(content).hexdigest(),prior=encode(prior),next=encode(following),feed='feed')
        clock='2026-10-12T00:00:00.123456+00:00';columns=fixture_columns(RESOURCE_ROOT)
        state,steps=graph_sql_plan(empty_state(),batch,{role:'local.notes.'+role for role in TABLES},materialized_at=clock,schema_policy=lambda change:None)
        expected=note_expected_rows({role:[]for role in columns},state,extracted.changes,batch,columns,clock,extracted.event_bytes)
        self.assertEqual(expected['object_current'][0]['published_at'],'1791763200123456')
        rows=[row for step in steps if 'rows'in step['parameters']for row in json.loads(step['parameters']['rows'])]
        proposed=next(row for row in rows if 'props_json'in row)
        actual=expected['object_current'][0]
        for key,value in proposed.items():
            if key!='published_at':self.assertEqual(actual[key],value)
        self.assertEqual(base64.b64decode(expected['whole_source_history'][0]['raw_base64']),raw)

    def test_ordinary_session_factory_requires_original_context(self):
        from ashlar_host.truss_note_publication import bind_ordinary_connection
        context=object();connection=object();calls=[]
        def factory():calls.append('opened');return connection
        selected=bind_ordinary_connection(factory,context)
        with self.assertRaises(PermissionError):selected(object())
        self.assertEqual(calls,[]);self.assertIs(selected(context),connection)
        self.assertEqual(calls,['opened'])
