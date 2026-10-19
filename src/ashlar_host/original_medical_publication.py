# SPDX-License-Identifier: MIT OR Apache-2.0
"""Historical medical admission and independent native oracle; no publisher entry yet."""
from dataclasses import dataclass
import base64
import datetime
import decimal
import hashlib
import json
from pathlib import Path
from ashlar.medical_source import SOURCE_SHA,GRAPH_SHA,PUBLIC_SHA,UMF_PIN,build_transaction
from ashlar.source import SourceBatch,records_digest
from ashlar.whole_entity import changes_from_batch
from ashlar.apply import Change
from .lifecycle import finish
from .medical_producer import MedicalFile,HeldMedicalDataset,decode_medical,verify_medical_revalidation


@dataclass(frozen=True)
class MedicalAdmissionEvidence:
    model:bytes
    graph:bytes
    baseline:bytes
    bindings:bytes
    revalidation:bytes
    def __post_init__(self):
        if any(type(raw)is not bytes or not raw for raw in (self.model,self.graph,self.baseline,self.bindings,self.revalidation))or sum(map(len,(self.model,self.graph,self.baseline,self.bindings,self.revalidation)))>12582912:raise ValueError('Complete bounded original medical evidence required')


class MedicalAdmission:
    """Exact original finite admission, separately renewed public recomputation."""
    def __init__(self,batch,bindings,model_path,graph_path,receipt_path,producer):
        if type(batch)is not SourceBatch or type(bindings)is not dict or type(producer)is not HeldMedicalDataset:raise ValueError('Exact medical batch/bindings/public producer required')
        self.files=tuple(MedicalFile(p,4194304)for p in (model_path,graph_path,receipt_path))
        if sum(f.size for f in self.files)>4194304:raise ValueError('Aggregate medical admission input exceeded')
        self.originals=tuple(f.read()for f in self.files);self.producer=producer
        self.revalidation=producer(*self.originals)
        verify_medical_revalidation(*self.originals,self.revalidation)
        rebuilt,expected=build_transaction(*self.originals,source_system=batch.feed,binding_profile='ashlar-medical-development-bindings/0.2')
        if rebuilt!=batch or expected!=bindings:raise PermissionError('Complete original medical batch/bindings differ')
        self.batch=batch;self.original_batch=batch;self.source_transaction_sha256=hashlib.sha256(batch.begin+b''.join(record.raw for record in batch.records)+batch.commit).hexdigest();self.bindings_bytes=json.dumps(bindings,ensure_ascii=False,separators=(',',':')).encode();self.changes=changes_from_batch(batch)
        if len(self.changes)!=113 or any(c.operation!='create'or c.state.schema_revision!=SOURCE_SHA for c in self.changes):raise PermissionError('Complete original medical creates required')
    def renew(self):
        if type(self.batch)is not SourceBatch or self.batch!=self.original_batch:raise PermissionError('Original medical source batch changed')
        def input(f,original):
            if f.read()!=original:raise PermissionError('Original medical admission drift')
        def producer():
            if type(self.producer)is not HeldMedicalDataset:raise PermissionError('Exact current medical producer owner required')
            if self.producer(*self.originals)!=self.revalidation:raise PermissionError('Original medical revalidation drift')
        finish(None,[lambda f=f,raw=raw:input(f,raw)for f,raw in zip(self.files,self.originals)]+[producer])
    def evidence(self):
        """Complete original raw views, separately bounded; no metadata truncation."""
        self.renew()
        return MedicalAdmissionEvidence(*self.originals,self.bindings_bytes,self.revalidation)
    def metadata(self):
        self.renew()
        return {'profile':'ashlar-original-medical-public-dataset-admission/0.1','declared_umf':'0.8.0','umf_revision':UMF_PIN,'source_sha256':SOURCE_SHA,'graph_sha256':GRAPH_SHA,'public_receipt_sha256':PUBLIC_SHA,'source_transaction_sha256':self.source_transaction_sha256,'bindings_sha256':hashlib.sha256(self.bindings_bytes).hexdigest(),'revalidation_sha256':hashlib.sha256(self.revalidation).hexdigest(),'evidence':{'profile':'ashlar-original-medical-admission-evidence/0.1','files':[{'role':role,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}for role,raw in zip(('model','graph','baseline','bindings','revalidation'),(*self.originals,self.bindings_bytes,self.revalidation))]},'record_count':51,'edge_count':62,'history_count':113,'boolean_receipt_count':3,'dataset_valid':True,'dataset_complete':True,'qualification':'Supplied historical medical dataset only; complete raw warnings/unknowns/provenance available through evidence(); no native/source/ACK authority'}
    def admit(self,change):
        self.renew()
        if type(change)is not Change or change not in self.changes:raise PermissionError('No admitted original medical change')


def original_medical_oracle(model_bytes,graph_bytes,bindings,*,feed,epoch,batch_id,materialized_at,columns):
    """Independent original graph/model → full native row oracle.

    Reads original lexical values and declared members directly. Does not consume
    build_transaction event projections, graph_sql_plan results or native rows.
    Bindings are separately admitted development IDs, not semantic validation.
    """
    if hashlib.sha256(model_bytes).hexdigest()!=SOURCE_SHA or hashlib.sha256(graph_bytes).hexdigest()!=GRAPH_SHA:raise ValueError('Exact original oracle bytes required')
    model=json.loads(model_bytes);graph=json.loads(graph_bytes);elements={(m['id'],e['id']):e for m in model['modules'] for e in m['elements']}
    if bindings['profile']=='ashlar-medical-development-bindings/0.2':
        properties=bindings['properties'];expected_fields={(model['id'],m,e) for (m,e),field in elements.items() if field['kind']=='field'}
        if len(properties)!=len(expected_fields) or {tuple(p['identity']) for p in properties}!=expected_fields or len({p['property_id'] for p in properties})!=len(properties) or any(str(int(p['property_id']))!=p['property_id'] or not 0<int(p['property_id'])<2**63 for p in properties):raise ValueError('Injective complete canonical development Field binding required')
    else:raise ValueError('Unknown development binding profile')
    resources={o['values']['resources.resource_key']:json.loads(o['values']['resources.resource_json'],parse_float=decimal.Decimal) for o in graph['objects'] if o['type']['element']=='resources'}
    assigned={(b['kind'],b['originalKey']):b for b in bindings['entities']}
    if len(assigned)!=113 or {(k,key) for k,key in assigned}!={('object',o['key']) for o in graph['objects']}|{('edge',e['key']) for e in graph['edges']}:raise ValueError('Injective complete original binding inventory required')
    if len({(b['kind'],b['type_id'],b['id']) for b in bindings['entities']})!=113:raise ValueError('Distinct development carrier identities required')
    instant=datetime.datetime.fromisoformat(materialized_at)
    if instant.tzinfo is None or instant.utcoffset()!=datetime.timedelta(0):raise ValueError('Explicit UTC materialization instant required')
    delta=instant-datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc);stamp=str((delta.days*86400+delta.seconds)*1000000+delta.microseconds)
    if type(feed)is not str or not feed or epoch!='original-medical-v1' or batch_id!='original-medical-v1' or set(columns)!={'object_current','edge_current','tombstone','whole_source_history'}:raise ValueError('Exact original medical scope and data roles required')
    result={role:[] for role in columns};cursor=None
    def text(value):return json.dumps(value,ensure_ascii=False,separators=(',',':'))
    for kind,rows in [('object',graph['objects']),('edge',graph['edges'])]:
        role='object_current' if kind=='object' else 'edge_current';typed='type_id' if kind=='object' else 'rel_type_id'
        for ordinal,original in enumerate(rows,1):
            binding=assigned[(kind,original['key'])];identity={'source_system':feed,typed:int(binding['type_id']),'id':int(binding['id'])}
            props=[]
            if kind=='object':
                record=elements[(original['type']['module'],original['type']['element'])]
                for ref in record['members']:
                    field=elements[(ref['module'],ref['element'])];lexical=original['values'][ref['element']]
                    property_key=ref['element']
                    if bindings['profile']=='ashlar-medical-development-bindings/0.2':
                        candidates=[p for p in bindings['properties'] if p['identity']==[model['id'],ref['module'],ref['element']]]
                        if len(candidates)!=1:raise ValueError('Exact original qualified Field binding required')
                        property_key=candidates[0]['property_id']
                    if lexical is None:literal='null'
                    elif field['scalarType']=='boolean':
                        resource_key=original['values'][original['type']['element']+'.resource_key'];native=resources[resource_key].get(field['name'])
                        if type(native) is not bool or lexical not in ('true','false','True','False') or (lexical in ('true','True')) is not native:raise ValueError('Original native resource Boolean/CSV token witness differs')
                        literal=text(native)
                    elif field['scalarType']=='string':literal=text(lexical)
                    else:literal=lexical
                    props.append(text(property_key)+':'+literal)
            props_json='{'+','.join(props)+'}'
            retained=text({'profile':bindings['profile'],'sourceSha256':SOURCE_SHA,'graphSha256':GRAPH_SHA,'publicReceiptSha256':PUBLIC_SHA,'original':original})
            delivery=kind+':'+str(ordinal)
            row={name:None for name,_ in columns[role]};row.update({k:str(v) for k,v in identity.items()})
            row.update(schema_revision=SOURCE_SHA,entity_version='1',props_json=props_json,retained_json=retained,source_feed=feed,source_epoch=epoch,source_cursor_json=cursor,source_delivery_id=delivery,published_at=stamp,lookup_hash=hashlib.sha256(text(identity).encode()).hexdigest(),apply_batch_id=batch_id)
            endpoints=None
            if kind=='object':row['logical_key_json']='[]'
            else:
                a=assigned[('object',original['source'])];b=assigned[('object',original['target'])]
                row.update(source_type=a['type_id'],source_id=a['id'],target_type=b['type_id'],target_id=b['id'])
                endpoints=[{'source':feed,'kind':'object','type_id':int(x['type_id']),'id':int(x['id'])} for x in (a,b)]
            result[role].append(row)
            event={'kind':'event','delivery_id':delivery,'source_profile':'ashlar-whole-entity/0.1','source_system':feed,'entity_kind':kind,'type_id':binding['type_id'],'id':binding['id'],'entity_version':'1','schema_revision':SOURCE_SHA,'operation':'create','props_json':props_json,'retained_json':retained}
            if kind=='edge':event['endpoints']=[{'type_id':x['type_id'],'id':x['id']}for x in (a,b)]
            raw=(text(event)+'\n').encode();raw_digest=hashlib.sha256(raw).hexdigest()
            state={'key':{'source':feed,'kind':kind,'type_id':int(binding['type_id']),'id':int(binding['id'])},'version':1,'schema_revision':SOURCE_SHA,'props_json':props_json,'retained_json':retained,'endpoints':endpoints}
            change={'feed':feed,'epoch':epoch,'delivery_id':delivery,'raw_digest':raw_digest,'operation':'create','state':state}
            result['whole_source_history'].append({'feed':feed,'epoch':epoch,'delivery_id':delivery,'digest':raw_digest,'change_json':json.dumps(change,separators=(',',':')),'raw_base64':base64.b64encode(raw).decode()})
    records=[base64.b64decode(row['raw_base64'])for row in result['whole_source_history']]
    begin=(text({'kind':'begin','batch_id':batch_id})+'\n').encode()
    commit=(text({'kind':'commit','batch_id':batch_id,'record_count':113,'records_sha256':records_digest(records)})+'\n').encode()
    cursor=text({'profile':'ashlar-jsonl-transactions/0.1','offset':str(len(begin)+sum(map(len,records))+len(commit))})
    for role in ('object_current','edge_current'):
        for row in result[role]:row['source_cursor_json']=cursor
    return result
