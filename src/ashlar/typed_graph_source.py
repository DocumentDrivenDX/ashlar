# SPDX-License-Identifier: MIT OR Apache-2.0
"""Exact graph request/policy custody; UMF owns Record/dataset semantics.

Raw post-group materialization proposes input only. Strict core apply and replay
run after original public receipts are attached, never with a permissive policy.
"""
import base64,hashlib,json
from .schema import decode_retained_json
from .source import jsonl_batches
from .whole_entity import changes_from_batch
from .typed_source_policy import typed_values
GRAPH_CHECK_PIN='2ede4c879bea5db13794a6fa30e67c821306c17f'

def require(value):
    if not value:raise ValueError('Original typed graph correspondence refused')

PROFILE='ashlar-typed-graph-check-request/0.1'
def encoded(value):return json.dumps(value,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def b64(raw):return base64.b64encode(raw).decode()
def instance(key):return encoded([key.source,key.kind,str(key.type_id),str(key.id)]).decode()
def record_request(change,bound):
    module,element=bound['record'];state=change.state
    return {'deliveryId':change.delivery_id,'recordSha256':change.raw_digest,'typeId':state.key.type_id,'propsSha256':sha(state.props_json.encode()),
        'identity':{'module':module,'element':element},'values':typed_values(state.props_json,bound['fields'])}

def _build(config):
    batches=tuple(jsonl_batches(config.source_bytes.splitlines(keepends=True),feed=config.feed,epoch=config.epoch));require(1<=len(batches)<=4)
    model=decode_retained_json(config.model_source);elements={(m['id'],e['id']):e for m in model['modules']for e in m['elements']}
    live={};groups=[];total=0
    for batch in batches:
        changes=changes_from_batch(batch);total+=len(changes);require(0<len(changes)and total<=1000 and len({c.state.key for c in changes})==len(changes))
        records=[]
        for change in changes:
            state=change.state;key=state.key
            require(key.source==config.source_system and state.schema_revision==config.document_revision and change.operation in ('create','replace','delete'))
            if key.kind=='object':
                require(str(key.type_id)in config.bindings);records.append(record_request(change,config.bindings[str(key.type_id)]))
            else:
                require(str(key.type_id)in config.relationships and decode_retained_json(state.props_json.encode())=={})
                bound=config.relationships[str(key.type_id)]
                require(state.endpoints is not None and tuple(e.type_id for e in state.endpoints)==(bound['source_type_id'],bound['target_type_id']))
            if change.operation=='delete':live.pop(key,None)
            else:live[key]=state
        dataset_records=[];links=[]
        for key,state in sorted(live.items()):
            if key.kind!='object':continue
            bound=config.bindings[str(key.type_id)];module,element=bound['record']
            dataset_records.append({'instanceId':instance(key),'identity':{'module':module,'element':element},'values':typed_values(state.props_json,bound['fields'])})
        for key,state in sorted(live.items()):
            if key.kind!='edge':continue
            bound=config.relationships[str(key.type_id)];start,end=state.endpoints
            require(start in live and end in live)
            target=config.bindings[str(end.type_id)];target_values=typed_values(live[end].props_json,target['fields'])
            values={(v['field']['module'],v['field']['element']):v['value']for v in target_values}
            module,element,key_id=bound['target_key'];definition=next(k for k in elements[module,element]['keys']if k['id']==key_id)
            keyvalues=[]
            for field in definition['fields']:
                value=values.get((field['module'],field['element']));require(type(value)is dict);keyvalues.append(value)
            links.append({'instanceId':instance(key),'identity':{'module':bound['identity'][0],'id':bound['identity'][1]},'sourceInstanceId':instance(start),
                'target':{'identity':{'module':module,'element':element,'key':key_id},'values':keyvalues}})
        metadata={'feed':batch.feed,'epoch':batch.epoch,'batchId':batch.batch_id,'recordCount':len(batch.records),'recordsSha256':batch.records_sha256,
            'cursorBefore':batch.cursor_before,'cursorAfter':batch.cursor_after,'beginBase64':b64(batch.begin),'commitBase64':b64(batch.commit)}
        groups.append({'batch':metadata,'events':[{'deliveryId':r.delivery_id,'recordSha256':r.sha256,'rawBase64':b64(r.raw)}for r in batch.records],
            'records':records,'dataset':{'scope':{'id':encoded([config.feed,config.epoch,batch.batch_id,batch.records_sha256]).decode(),'closure':'supplied-dataset-only'},'records':dataset_records,'relationships':links}})
    request={'profile':PROFILE,'configurationSha256':sha(config.original),'sourceSha256':sha(config.source_bytes),'bindingSha256':sha(config.binding_source),'modelSha256':sha(config.model_source),
        'documentRevision':config.document_revision,'sourceSystem':config.source_system,'feed':config.feed,'epoch':config.epoch,'groups':groups}
    raw=encoded(request);require(len(raw)<=1048576);return raw,batches

def build_typed_graph_request(configuration):
    configuration.verify();raw,_=_build(configuration);configuration.verify();return raw

def _valid(value):return type(value)is dict and set(value)=={'valid','complete','diagnostics'}and value['valid']is True and value['complete']is True and value['diagnostics']==[]
def _record_validation(value,record,source):
    diagnostics=[]
    if record.get('keys'):
        diagnostics.append({'code':'RECORD_KEY_CONTEXT_REQUIRED','path':'/identity','message':'Dataset key equality/uniqueness needs a separately qualified check','severity':'warning'})
    if any(m.get('relationships')for m in source['modules']):
        diagnostics.append({'code':'RECORD_RELATIONSHIP_CONTEXT_REQUIRED','path':'/identity','message':'Relationship/endpoint constraints need a separately qualified dataset check','severity':'warning'})
    return type(value)is dict and set(value)=={'valid','complete','diagnostics'}and value['valid']is True and value['complete']is (not diagnostics)and value['diagnostics']==diagnostics
def verify_typed_graph_receipt(model,request,receipt):
    """Original envelope/result correspondence, not producer authentication.

    The host owns actual public core0.10 execution and verifyCoreDatasetValuesCompact.
    Importing a receipt alone cannot establish that execution or source authority.
    """
    require(type(model)is bytes and 0<len(model)<=65536 and type(request)is bytes and 0<len(request)<=1048576 and type(receipt)is bytes and 0<len(receipt)<=4194304)
    source=decode_retained_json(model);wanted=decode_retained_json(request);report=decode_retained_json(receipt)
    require(source.get('umf')=='0.10.0')
    require(type(report)is dict and set(report)=={'format','producerRevision','sourceBase64','sourceSha256','requestBase64','requestSha256','groups','qualification'}
        and report['format']=='ashlar-typed-graph-check/0.1'and report['producerRevision']==GRAPH_CHECK_PIN and type(report['qualification'])is str)
    require(report['sourceBase64']==b64(model)and report['sourceSha256']==sha(model)and report['requestBase64']==b64(request)and report['requestSha256']==sha(request)
        and wanted['profile']==PROFILE and wanted['modelSha256']==sha(model)and type(report['groups'])is list and len(report['groups'])==len(wanted['groups']))
    elements={(m['id'],e['id']):e for m in source['modules']for e in m['elements']}
    key_paths={(m['id'],e['id'],k['id']):'/modules/'+str(mi)+'/elements/'+str(ei)+'/keys/'+str(ki)for mi,m in enumerate(source['modules'])for ei,e in enumerate(m['elements'])for ki,k in enumerate(e.get('keys',[]))}
    live_edges={};source_parts=[]
    for group,observed in zip(wanted['groups'],report['groups']):
        metadata=group['batch'];raw=base64.b64decode(metadata['beginBase64'],validate=True)+b''.join(base64.b64decode(e['rawBase64'],validate=True)for e in group['events'])+base64.b64decode(metadata['commitBase64'],validate=True)
        source_parts.append(raw)
        batches=tuple(jsonl_batches(raw.splitlines(keepends=True),feed=wanted['feed'],epoch=wanted['epoch'],cursor_before=metadata['cursorBefore']))
        require(len(batches)==1 and batches[0].cursor_after==metadata['cursorAfter'])
        for change in changes_from_batch(batches[0]):
            if change.state.key.kind=='edge':
                if change.operation=='delete':live_edges.pop(instance(change.state.key),None)
                else:live_edges[instance(change.state.key)]=tuple(instance(e)for e in change.state.endpoints)

        require(type(observed)is dict and set(observed)=={'records','dataset'}and type(observed['records'])is list and len(observed['records'])==len(group['records']))
        for item,entry in zip(group['records'],observed['records']):
            require(type(entry)is dict and set(entry)=={'request','result'}and entry['request']==item)
            result=entry['result'];require(type(result)is dict and result.get('operation')=='validate-core-record-values'and result.get('version')=='2.0.0'
                and result.get('source')==source and result.get('identity')==item['identity']and result.get('values')==item['values']and _record_validation(result.get('validation'),elements[item['identity']['module'],item['identity']['element']],source))
            document=result.get('documentValidation');require(_valid(document))
            members=elements[item['identity']['module'],item['identity']['element']]['members'];fields=result.get('fields')
            require(type(fields)is list and len(fields)==len(members));states={(v['field']['module'],v['field']['element']):v['state']for v in item['values']};require(len(states)==len(item['values']))
            for member,field in zip(members,fields):require(type(field)is dict and set(field)=={'field','state','validation'}and field['field']==member and field['state']==states.get((member['module'],member['element']),'absent')and _valid(field['validation']))
        dataset=observed['dataset'];require(type(dataset)is dict and set(dataset)=={'operation','version','scope','provenance','source','input','documentValidation','records','keys','relationships','datasetValidation','obligations','residuals'}and dataset.get('operation')=='validate-core-dataset-values-compact'and dataset.get('version')=='1.0.0'
            and dataset.get('source')==source and dataset.get('input')==group['dataset']and dataset.get('scope')=='supplied-dataset-only'and dataset.get('provenance')=='unverified'
            and _valid(dataset.get('datasetValidation'))and dataset.get('residuals')==[]
            and dataset.get('obligations')==[{'id':n,'state':'satisfied','scope':'supplied-dataset-only'}for n in ('dataset.keys','dataset.relationships')])

        document=dataset['documentValidation'];require(_valid(document))
        require(all(r['result']['documentValidation']==document for r in observed['records']))
        expected_records=group['dataset']['records'];actual_records=dataset.get('records')
        require(type(actual_records)is list and len(actual_records)==len(expected_records))
        for item,entry in zip(expected_records,actual_records):
            require(type(entry)is dict and set(entry)=={'instanceId','result'}and entry['instanceId']==item['instanceId'])
            result=entry['result'];require(type(result)is dict and 'source'not in result and result.get('sourceRef')=='#/source'
                and result.get('operation')=='validate-core-record-values'and result.get('version')=='2.0.0'
                and result.get('documentValidation')==document and result.get('identity')==item['identity']and result.get('values')==item['values']and _record_validation(result.get('validation'),elements[item['identity']['module'],item['identity']['element']],source))
            members=elements[item['identity']['module'],item['identity']['element']]['members'];fields=result.get('fields')
            require(type(fields)is list and len(fields)==len(members))
            states={(v['field']['module'],v['field']['element']):v['state']for v in item['values']}
            for member,field in zip(members,fields):require(type(field)is dict and set(field)=={'field','state','validation'}and field['field']==member and field['state']==states.get((member['module'],member['element']),'absent')and _valid(field['validation']))
        expected_keys=[]
        for item in expected_records:
            identity=item['identity'];values={(v['field']['module'],v['field']['element']):v['value']for v in item['values']}
            for key in elements[identity['module'],identity['element']].get('keys',[]):
                expected_keys.append((item['instanceId'],dict(identity,key=key['id']),[values[(f['module'],f['element'])]for f in key['fields']]))
        actual_keys=dataset.get('keys');require(type(actual_keys)is list and len(actual_keys)==len(expected_keys))
        def key_receipt(result,identity,values):
            require(type(result)is dict and set(result)=={'operation','version','profile','sourceRef','identity','values','keyPath','bytesHex'}
                and result['operation']=='encode-core-key-tuple'and result['version']=='4.0.0'and result['profile']=='umf-key-tuple-v1'
                and result['sourceRef']=='#/source'and result['identity']==identity and result['values']==values
                and result['keyPath']==key_paths[(identity['module'],identity['element'],identity['key'])]and type(result['bytesHex'])is str and 0<len(result['bytesHex'])and len(result['bytesHex'])%2==0
                and all(c in '0123456789abcdef'for c in result['bytesHex']))
        for entry,(instance_id,identity,values)in zip(actual_keys,expected_keys):
            require(type(entry)is dict and set(entry)=={'instanceId','result'}and entry['instanceId']==instance_id);key_receipt(entry['result'],identity,values)
        expected_links=group['dataset']['relationships'];actual_links=dataset.get('relationships');record_ids={r['instanceId']for r in expected_records}
        require(type(actual_links)is list and len(actual_links)==len(expected_links))
        for link,entry in zip(expected_links,actual_links):
            require(type(entry)is dict and set(entry)=={'instanceId','identity','sourceInstanceId','targetInstanceId','targetKey'}
                and all(entry[k]==link[k]for k in ('instanceId','identity','sourceInstanceId'))and entry['targetInstanceId']in record_ids
                and (entry['sourceInstanceId'],entry['targetInstanceId'])==live_edges.get(entry['instanceId']))
            key_receipt(entry['targetKey'],link['target']['identity'],link['target']['values'])

    require(sha(b''.join(source_parts))==wanted['sourceSha256'])

class TypedGraphSourcePolicy:
    """Exact carried-event policy minted only against complete original receipts."""
    def __init__(self,configuration,receipt):
        request,batches=_build(configuration);verify_typed_graph_receipt(configuration.model_source,request,receipt)
        self.request_bytes=request;self.receipt=receipt;self.source=configuration.model_source
        self.bindings=configuration.bindings;self.relationships=configuration.relationships
        self.source_system=configuration.source_system;self.schema_revision=configuration.document_revision
        self._changes={(c.feed,c.epoch,c.delivery_id):c for b in batches for c in changes_from_batch(b)}
        require(len(self._changes)==sum(len(b.records)for b in batches))
        from .apply import empty_state,plan_apply
        state=empty_state()
        for batch in batches:state=plan_apply(state,changes_from_batch(batch),schema_policy=self)
        replay=state
        for batch in batches:replay=plan_apply(replay,changes_from_batch(batch),schema_policy=self)
        require(replay==state)
    def __call__(self,change):
        require(self._changes.get((change.feed,change.epoch,change.delivery_id))==change)
