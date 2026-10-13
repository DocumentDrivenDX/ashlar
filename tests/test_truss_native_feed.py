import base64,hashlib,json,unittest
from ashlar.truss_native_feed import scalar_object_changes,native_feed_batch,NativeFeedError

class Tests(unittest.TestCase):
    def case(self,operation='metadata',version='2'):
        encode=lambda value:json.dumps(value).encode()
        cursor={'sourceEpoch':'epoch','installationId':'installation'};prior=encode(cursor);next=encode(cursor)
        identity={'id':'7','typeId':'3','definitionPin':'type-pin'}
        image={'interfaceVersion':'truss-history-record/0.1.0','kind':'object','identity':identity,'recordVersion':version,'ownership':{'state':'rootless'},'properties':[{'propertyId':'9','definitionPin':'property-pin','value':{'kind':'string','text':'exact'}}],'retained':[]}
        event={'interfaceVersion':'truss-history-event/0.2.0-proposal','operation':operation,'sourceEpoch':'epoch','xid':'42','seq':'987','eventVersion':version,'eventCatalogRevision':'2','identity':identity,'after':image}
        raw=encode(event);wrapper={'interfaceVersion':'truss-journal-exact-bytes/0.2.0-proposal','component':'event','bytesBase64':base64.b64encode(raw).decode(),'bytesSha256':hashlib.sha256(raw).hexdigest()}
        payload={'interfaceVersion':'truss-native-feed/0.1','prior':cursor,'next':cursor,'writerXid':'42','changes':[{'seq':987,'xid':'42','entity_kind':'o','entity_id':7,'entity_type':3,'ver':int(version),'new_value':wrapper}],'catalogPrerequisites':[{'original':'preserved'}]}
        content=encode(payload)
        return content,{'sha256':hashlib.sha256(content).hexdigest(),'prior':prior,'next':next,'feed':'selected-feed'}
    def test_full_afterimage_uses_native_version_not_sequence(self):
        content,kwargs=self.case();result=scalar_object_changes(content,**kwargs)
        self.assertEqual(result.original,content);self.assertEqual(result.changes[0].state.version,2)
        self.assertEqual(result.changes[0].operation,'replace');self.assertEqual(json.loads(result.changes[0].state.props_json),{'9':'exact'})
    def test_incomplete_property_only_transaction_refuses(self):
        content,kwargs=self.case('property')
        with self.assertRaises(NativeFeedError):scalar_object_changes(content,**kwargs)
    def test_original_digest_mismatch_refuses(self):
        content,kwargs=self.case();kwargs['sha256']='0'*64
        with self.assertRaises(NativeFeedError):scalar_object_changes(content,**kwargs)

    def test_create_update_delete_apply_as_entity_versions(self):
        from ashlar.apply import empty_state,plan_apply
        state=empty_state()
        for operation,version,xid in [('create','1','42'),('metadata','2','43'),('delete','3','44')]:
            content,kwargs=self.case(operation,version);payload=json.loads(content)
            row=payload['changes'][0];wrapper=row['new_value']
            event=json.loads(base64.b64decode(wrapper['bytesBase64']))
            payload['writerXid']=row['xid']=event['xid']=xid
            if operation=='delete':
                event['before']=event.pop('after');event['before']['recordVersion']='2'
            raw=json.dumps(event).encode()
            wrapper.update(bytesBase64=base64.b64encode(raw).decode(),bytesSha256=hashlib.sha256(raw).hexdigest())
            content=json.dumps(payload).encode();kwargs['sha256']=hashlib.sha256(content).hexdigest()
            selected=scalar_object_changes(content,**kwargs)
            state=plan_apply(state,selected.changes,schema_policy=lambda change:None)
        self.assertEqual(len(state.current),0)
        self.assertEqual(len(state.history),3)
        self.assertEqual(next(iter(state.tombstones.values())).state.version,3)

    def test_native_whole_page_batch_preserves_opaque_cursor_and_unknown_content(self):
        content,kwargs=self.case();payload=json.loads(content)
        before={'profile':'truss-caller-asserted-feed/0.1','sourceEpoch':'epoch','installationId':'installation','consumerId':'consumer','registrationId':'registration','nextXid':'0'}
        after={**before,'nextXid':'43'}
        payload.update(prior=before,next=after,configuration={},revisions=[],sources=[],reservations=[],unknown={'native_number':1.25})
        content=json.dumps(payload).encode();kwargs.update(sha256=hashlib.sha256(content).hexdigest(),prior=json.dumps(before).encode(),next=json.dumps(after).encode())
        batch=native_feed_batch(content,**kwargs)
        selected=scalar_object_changes(content,**kwargs)
        self.assertEqual(selected.original,content)
        self.assertEqual(selected.changes[0].state.props_json,'{"9":"exact"}')
        self.assertEqual(batch.profile,'truss-native-feed/0.1');self.assertEqual(batch.begin,content)
        self.assertEqual(batch.records,());self.assertEqual(batch.batch_id,'42')
        self.assertEqual(base64.b64decode(batch.cursor_before),kwargs['prior'])
        from ashlar.staging import batch_row,batch_from_row,StagingError
        from ashlar.source_checkpoint import truss_checkpoint,validate_source_checkpoint,CheckpointError
        row=batch_row(batch)
        self.assertEqual(batch_from_row(row),batch)
        checkpoint=truss_checkpoint(batch)
        self.assertEqual(validate_source_checkpoint(checkpoint,batch)['position'],batch.cursor_after)
        changed=json.loads(checkpoint);changed['position']=changed['previous']
        with self.assertRaises(CheckpointError):validate_source_checkpoint(json.dumps(changed),batch)
        altered={**row,'cursor_after':row['cursor_before']}
        with self.assertRaises(StagingError):batch_from_row(altered)

    def test_public_exact_string_tags_decode_but_other_values_refuse(self):
        import base64
        content,kwargs=self.case();payload=json.loads(content);wrapper=payload['changes'][0]['new_value'];event=json.loads(base64.b64decode(wrapper['bytesBase64']))
        event['after']['properties'][0]['value']={'kind':'string','text':'雪 exact'}
        event['after']['retained']=[{'name':'note','value':{'kind':'string','text':'retained text'}}]
        def selected():
            raw=json.dumps(event).encode();wrapper.update(bytesBase64=base64.b64encode(raw).decode(),bytesSha256=hashlib.sha256(raw).hexdigest())
            body=json.dumps(payload).encode();return body,{**kwargs,'sha256':hashlib.sha256(body).hexdigest()}
        body,args=selected();parsed=scalar_object_changes(body,**args)
        self.assertEqual(json.loads(parsed.changes[0].state.props_json),{'9':'雪 exact'})
        self.assertEqual(json.loads(parsed.changes[0].state.retained_json),{'note':'retained text'})
        self.assertEqual(json.loads(parsed.event_bytes[0])['after']['properties'][0]['value'],{'kind':'string','text':'雪 exact'})
        for value in ('legacy plain string',{'kind':'integer','text':'1'},{'kind':'string','text':1},{'kind':'string','text':'x','unknown':True}):
            with self.subTest(value=value):
                event['after']['properties'][0]['value']=value;body,args=selected()
                with self.assertRaises(NativeFeedError):scalar_object_changes(body,**args)
        event['after']['properties'][0]['value']={'kind':'string','text':'valid'};event['after']['retained'][0]['value']={'kind':'boolean','value':True};body,args=selected()
        with self.assertRaises(NativeFeedError):scalar_object_changes(body,**args)

    def typed_case(self,values):
        content,args=self.case();payload=json.loads(content);wrapper=payload['changes'][0]['new_value'];event=json.loads(base64.b64decode(wrapper['bytesBase64']))
        event['after']['properties']=[{'propertyId':str(i+1),'definitionPin':'pin','value':value}for i,value in enumerate(values)]
        event['after']['retained']=[{'name':'exact','value':values[0]}]
        raw=json.dumps(event).encode();wrapper.update(bytesBase64=base64.b64encode(raw).decode(),bytesSha256=hashlib.sha256(raw).hexdigest())
        body=json.dumps(payload).encode();return body,{**args,'sha256':hashlib.sha256(body).hexdigest()},raw

    def test_typed_scalar_tokens_remain_exact_without_float_conversion(self):
        from ashlar.truss_native_feed import typed_scalar_entity_changes
        values=[{'kind':'integer','text':'9007199254740993'},{'kind':'decimal','text':'1.2300'},{'kind':'decimal','text':'-0e+2'},{'kind':'boolean','value':True},{'kind':'null'},{'kind':'string','text':'雪'}]
        content,args,event=self.typed_case(values);result=typed_scalar_entity_changes(content,**args)
        self.assertEqual(result.original,content);self.assertEqual(result.event_bytes,(event,))
        self.assertEqual(result.changes[0].state.props_json,'{"1":9007199254740993,"2":1.2300,"3":-0e+2,"4":true,"5":null,"6":"雪"}')
        self.assertEqual(result.changes[0].state.retained_json,'{"exact":9007199254740993}')
        with self.assertRaises(NativeFeedError):scalar_object_changes(content,**args)

    def test_typed_scalar_closed_families_and_numeric_grammar_refuse(self):
        from ashlar.truss_native_feed import typed_scalar_entity_changes
        for value in ({'kind':'integer','text':'01'},{'kind':'integer','text':'1.0'},{'kind':'decimal','text':'NaN'},{'kind':'decimal','text':' 1'},{'kind':'decimal','text':'1.'},{'kind':'boolean','value':1},{'kind':'null','extra':True},{'kind':'sequence','values':[]},{'kind':'map','entries':[]}):
            with self.subTest(value=value):
                content,args,_=self.typed_case([value])
                with self.assertRaises(NativeFeedError):typed_scalar_entity_changes(content,**args)
