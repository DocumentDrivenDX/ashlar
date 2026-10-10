"""Named commerce development transactions and independent original snapshot oracle.

This prepares data transitions; it does not validate UMF, authorize a source, or
admit native migration. A host must freshly execute the public preservation
checker and separately establish installation/writer/publication/ACK authority.
"""
import base64
from dataclasses import dataclass, replace
import datetime
import hashlib
import json
import re
from ashlar.source import jsonl_batches, records_digest
# Exact original model pin owned by this separately qualified evolution profile.
ORIGINAL_MODEL_SHA='51d87c554df36846e41378cfaafc81277fcab61f6c891a6c64f34dba8fd9ac2a'

CANDIDATE_SHA='450c82ef4b76fd73b095dc4617d0a9d2a9f9d06c15b9e402fc757be31b510aba'
PRESERVATION_SHA='bdd21b1566e66dbe5cb1fd2ff6e098853aa9766b4bf6cdb1cf5192ecd0191576'
PROFILE='ashlar-commerce-evolution-transactions/0.1'


def text(value):
    return json.dumps(value,ensure_ascii=False,separators=(',',':'),allow_nan=False)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def admitted_inputs(candidate_bytes,preservation_bytes,original_model_bytes):
    if (digest(candidate_bytes),digest(preservation_bytes),digest(original_model_bytes))!=(CANDIDATE_SHA,PRESERVATION_SHA,ORIGINAL_MODEL_SHA):
        raise ValueError('Exact original candidate/public proof/model byte custody required')
    candidate=json.loads(candidate_bytes);proof=json.loads(preservation_bytes)
    if proof.get('profile')!='ashlar-commerce-evolution-presence-public/0.1' or proof['candidate']!=candidate:
        raise ValueError('Named public presence candidate correspondence required')
    if json.loads(original_model_bytes)!=candidate['revisions'][0]['source']:
        raise ValueError('Original model correspondence differs')
    if proof['umfRevision']!='e44cd15f336dfb33db35acf20eee13dd120a1a28' or [x['receipt']['classification'] for x in proof['transitions']]!=['preserved']*3+['breaking']:
        raise ValueError('Exact public source-preservation profile required')
    return candidate,proof


def rows(graph):
    return {('object',r['key']):r for r in graph['objects']}|{('edge',r['key']):r for r in graph['edges']}


def model_bytes(revision,original):
    return original if revision['source']==json.loads(original) else (text(revision['source'])+'\n').encode()


def numeric_token(value):
    # A storage carrier restriction, after public semantic admission; no float.
    if type(value)is not str or not re.fullmatch(r'-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?',value):
        raise ValueError('Exact finite JSON numeric-token carrier required')
    return value


def properties_from_public_input(record,property_ids,document):
    parts=[]
    for value in record['values']:
        if value['state']=='absent':continue
        if value['state']!='present':raise ValueError('This finite input has no null literal carrier')
        literal=value['value'];field=value['field'];identity=(document,field['module'],field['element'])
        if set(literal)=={'string'}:carrier=text(literal['string'])
        elif set(literal) in ({'integerToken'},{'decimalToken'}):carrier=numeric_token(next(iter(literal.values())))
        else:raise ValueError('Explicit admitted original literal carrier required')
        parts.append(text(property_ids[identity])+':'+carrier)
    return '{'+','.join(parts)+'}'


@dataclass(frozen=True)
class PreparedEvolution:
    batches:tuple
    registry_json:str
    schema_revisions:tuple
    source_system:str
    epoch:str


def prepare(candidate_bytes,preservation_bytes,original_model_bytes,*,source_system,epoch,through_revision='R4-additive-candidate'):
    candidate,proof=admitted_inputs(candidate_bytes,preservation_bytes,original_model_bytes)
    if any(type(v)is not str or not v or '\x00' in v for v in (source_system,epoch)):
        raise ValueError('Explicit development namespace/epoch required')
    selected=[r['id'] for r in candidate['revisions'][:4]]
    if through_revision not in selected:raise ValueError('Breaking or unknown source revision cannot produce a transaction')
    count=selected.index(through_revision)+1
    registry=candidate['registry'];assign={(r['kind'],r['originalKey']):r for r in registry['entities']}
    field_ids={tuple(r['identity']):r['property_id'] for r in registry['properties']}
    versions={};previous={};retained_states={};batches=[];revisions=[]
    for index,revision in enumerate(candidate['revisions'][:count]):
        current=rows(revision['graph']);schema=digest(model_bytes(revision,original_model_bytes));revisions.append(schema)
        old_schema=revisions[index-1] if index else None
        changed=[key for key in current if key not in previous or current[key]!=previous[key] or schema!=old_schema]
        deleted=[key for key in previous if key not in current]
        operations=[(key,'delete',previous[key])for key in sorted(deleted,key=lambda k:(k[0]!='edge',k[1]))]
        operations += [(key,'create' if key not in previous else 'replace',current[key])for key in changed]
        inputs={r['instanceId']:r for r in proof['datasetChecks'][index]['input']['records']}
        events=[]
        for key,operation,row in operations:
            kind,original_key=key;binding=assign[key];versions[key]=versions.get(key,0)+1
            # Deletes retain the exact previous row and its prior typed values.
            value_input=(inputs if operation!='delete' else {r['instanceId']:r for r in proof['datasetChecks'][index-1]['input']['records']})
            props=properties_from_public_input(value_input[original_key],field_ids,revision['source']['id']) if kind=='object' else '{}'
            retained={'profile':PROFILE,'revision':revision['id'],'source_sha256':schema,'original_key':original_key,'operation':operation,'original':row}
            retained_text=retained_states[key] if operation=='delete' else text(retained)
            if operation!='delete':retained_states[key]=retained_text
            event={'kind':'event','delivery_id':revision['id']+':'+kind+':'+binding['id'],'source_profile':'ashlar-whole-entity/0.1','source_system':source_system,'entity_kind':kind,'type_id':binding['type_id'],'id':binding['id'],'entity_version':str(versions[key]),'schema_revision':schema,'operation':operation,'props_json':props,'retained_json':retained_text}
            if kind=='edge':event['endpoints']=[{k:assign[('object',row[end])][k] for k in ('type_id','id')} for end in ('source','target')]
            events.append((text(event)+'\n').encode())
        ident=revision['id'];begin=(text({'kind':'begin','batch_id':ident})+'\n').encode();commit=(text({'kind':'commit','batch_id':ident,'record_count':len(events),'records_sha256':records_digest(events)})+'\n').encode()
        batch,=jsonl_batches((begin,*events,commit),feed=source_system,epoch=epoch)
        batches.append(batch);previous=current
    return PreparedEvolution(tuple(batches),text(registry),tuple(revisions),source_system,epoch)


def independent_oracle(candidate_bytes,preservation_bytes,original_model_bytes,prepared,columns,*,prefix,materialized_at):
    """Original graph/Field definitions → native cells, independently of emitter/SQL.

    Source envelopes supply only raw digest/cursor/history custody. Current props,
    identities, topology, changes and versions derive from original snapshot rows.
    """
    candidate,_=admitted_inputs(candidate_bytes,preservation_bytes,original_model_bytes)
    if type(prefix)is not int or not 1<=prefix<=4:raise ValueError('Explicit finite prefix required')
    registry=candidate['registry']
    expected_schemas=tuple(digest(model_bytes(r,original_model_bytes))for r in candidate['revisions'][:len(prepared.batches)])
    if prepared.schema_revisions!=expected_schemas or len(prepared.batches)<prefix:raise ValueError('Original revision vector differs')
    if json.loads(prepared.registry_json)!=registry:raise ValueError('Original development registry changed')
    assigned={(b['kind'],b['originalKey']):b for b in registry['entities']}
    property_ids={tuple(p['identity']):p['property_id'] for p in registry['properties']}
    instant=datetime.datetime.fromisoformat(materialized_at)
    if instant.tzinfo is None or instant.utcoffset()!=datetime.timedelta(0):raise ValueError('Original UTC clock required')
    elapsed=instant-datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc);stamp=str((elapsed.days*86400+elapsed.seconds)*1000000+elapsed.microseconds)
    current_rows={};history=[];tombstones={};previous={};versions={};old_schema=None
    for i,revision in enumerate(candidate['revisions'][:prefix]):
        schema=digest(model_bytes(revision,original_model_bytes));graph_rows=rows(revision['graph']);batch=prepared.batches[i]
        if batch.feed!=prepared.source_system or batch.epoch!=prepared.epoch or batch.batch_id!=revision['id']:raise ValueError('Original source envelope identity differs')
        definitions={(m['id'],f['id']):f for m in revision['source']['modules'] for f in m['elements']}
        expected=[(k,'delete',previous[k])for k in sorted(previous.keys()-graph_rows.keys(),key=lambda k:(k[0]!='edge',k[1]))]
        expected += [(k,'create' if k not in previous else 'replace',v)for k,v in graph_rows.items()if k not in previous or v!=previous[k]or schema!=old_schema]
        if len(expected)!=len(batch.records):raise ValueError('Exact source event inventory differs')
        cursor=text({'profile':batch.profile,'offset':batch.cursor_after})
        for (key,operation,original),record in zip(expected,batch.records):
            kind,_=key;b=assigned[key];versions[key]=versions.get(key,0)+1
            identity={'source':prepared.source_system,'kind':kind,'type_id':int(b['type_id']),'id':int(b['id'])}
            props=[]
            if kind=='object':
                selected=original['type'];definition=definitions[(selected['module'],selected['element'])]
                for field in definition['members']:
                    name=field['element']
                    if name not in original['values']:continue
                    lexical=original['values'][name];declared=definitions[(field['module'],name)]
                    key_id=property_ids[(revision['source']['id'],field['module'],name)]
                    encoded_value=text(lexical) if declared['scalarType']=='string' else numeric_token(lexical)
                    props.append(text(key_id)+':'+encoded_value)
            props_json='{'+','.join(props)+'}'
            retained=text({'profile':PROFILE,'revision':revision['id'],'source_sha256':schema,'original_key':key[1],'operation':operation,'original':original})
            if operation=='delete':retained=current_rows[key][1]['retained_json']
            endpoint_bindings=[assigned[('object',original[end])]for end in ('source','target')]if kind=='edge'else None
            endpoints=[{'source':prepared.source_system,'kind':'object','type_id':int(x['type_id']),'id':int(x['id'])}for x in endpoint_bindings]if endpoint_bindings else None
            state={'key':identity,'version':versions[key],'schema_revision':schema,'props_json':props_json,'retained_json':retained,'endpoints':endpoints}
            delivery=revision['id']+':'+kind+':'+b['id']
            if record.delivery_id!=delivery:raise ValueError('Original raw source delivery correspondence differs')
            change={'feed':batch.feed,'epoch':batch.epoch,'delivery_id':delivery,'raw_digest':record.sha256,'operation':operation,'state':state}
            history.append({'feed':batch.feed,'epoch':batch.epoch,'delivery_id':delivery,'digest':record.sha256,'change_json':json.dumps(change,separators=(',',':')),'raw_base64':base64.b64encode(record.raw).decode()})
            if operation=='delete':
                current_rows.pop(key,None);row={name:None for name,_ in columns['tombstone']};row.update(source_system=prepared.source_system,entity_kind=kind,type_id=b['type_id'],id=b['id'],entity_version=str(versions[key]),source_feed=batch.feed,source_epoch=batch.epoch,source_cursor_json=cursor,source_delivery_id=delivery);tombstones[key]=row
            else:
                role='object_current'if kind=='object'else'edge_current';row={name:None for name,_ in columns[role]};native_identity={'source_system':prepared.source_system,'type_id'if kind=='object'else'rel_type_id':int(b['type_id']),'id':int(b['id'])};row.update({k:str(v)for k,v in native_identity.items()});row.update(schema_revision=schema,entity_version=str(versions[key]),props_json=props_json,retained_json=retained,source_feed=batch.feed,source_epoch=batch.epoch,source_cursor_json=cursor,source_delivery_id=delivery,published_at=stamp,lookup_hash=digest(text(native_identity).encode()),apply_batch_id=batch.batch_id)
                if kind=='object':row['logical_key_json']='[]'
                else:row.update(source_type=endpoint_bindings[0]['type_id'],source_id=endpoint_bindings[0]['id'],target_type=endpoint_bindings[1]['type_id'],target_id=endpoint_bindings[1]['id'])
                current_rows[key]=(role,row)
        previous=graph_rows;old_schema=schema
    result={role:[]for role in columns}
    for role,row in current_rows.values():result[role].append(row)
    result['whole_source_history']=history;result['tombstone']=list(tombstones.values())
    return result


# The selected envelope changes only transaction identity/cursor witnesses.
BATCH_IDENTITY_PROFILE = 'source-epoch-qualified/0.1'

def qualify(prepared):
    batches=[]
    for original in prepared.batches:
        identity=text([prepared.source_system,prepared.epoch,original.batch_id])
        records=tuple(r.raw for r in original.records)
        begin=(text({'kind':'begin','batch_id':identity})+'\n').encode()
        commit=(text({'kind':'commit','batch_id':identity,'record_count':len(records),'records_sha256':records_digest(records)})+'\n').encode()
        batch,=jsonl_batches((begin,*records,commit),feed=original.feed,epoch=original.epoch)
        if tuple((r.delivery_id,r.raw,r.sha256)for r in batch.records)!=tuple((r.delivery_id,r.raw,r.sha256)for r in original.records):raise ValueError('Qualified envelope changed original records')
        batches.append(batch)
    return replace(prepared,batches=tuple(batches))


def scoped_oracle(candidate_bytes,proof_bytes,model_bytes,scoped,columns,*,prefix,materialized_at):
    """Independent logical originals plus exact named envelope custody witnesses.

    Only metadata cells are derived from the new declared envelope; all entity
    values, presence, endpoints, retained payloads and histories use the original
    independent snapshot oracle, never native query results or emitted properties.
    """
    original=prepare(candidate_bytes,proof_bytes,model_bytes,source_system=scoped.source_system,epoch=scoped.epoch)
    if scoped.registry_json!=original.registry_json or scoped.schema_revisions!=original.schema_revisions or len(scoped.batches)!=len(original.batches):raise ValueError('Original development/source revision custody differs')
    witnesses={}
    for source_batch,qualified in zip(original.batches,scoped.batches):
        # Independently derive exact envelope identity/serialization/cursor from
        # original byte records, without invoking qualify() or reading SQL rows.
        identity=json.dumps([original.source_system,original.epoch,source_batch.batch_id],ensure_ascii=False,separators=(',',':'))
        begin=(json.dumps({'kind':'begin','batch_id':identity},ensure_ascii=False,separators=(',',':'))+'\n').encode()
        commit=(json.dumps({'kind':'commit','batch_id':identity,'record_count':len(source_batch.records),'records_sha256':source_batch.records_sha256},ensure_ascii=False,separators=(',',':'))+'\n').encode()
        if (qualified.profile,qualified.feed,qualified.epoch,qualified.batch_id,qualified.begin,qualified.commit,qualified.records_sha256)!=(source_batch.profile,source_batch.feed,source_batch.epoch,identity,begin,commit,source_batch.records_sha256):raise ValueError('Exact named source/epoch envelope differs')
        offset=len(begin)
        final_cursor=str(len(begin)+sum(len(r.raw)for r in source_batch.records)+len(commit))
        for before,after in zip(source_batch.records,qualified.records):
            offset+=len(before.raw)
            if (before.delivery_id,before.raw,before.sha256)!=(after.delivery_id,after.raw,after.sha256)or after.cursor!=str(offset):raise ValueError('Original record bytes/cursor correspondence differs')
            witnesses[before.delivery_id]={'batch_id':identity,'cursor':json.dumps({'profile':qualified.profile,'offset':final_cursor},separators=(',',':'))}
        if len(qualified.records)!=len(source_batch.records)or qualified.cursor_before!='0'or qualified.cursor_after!=str(offset+len(commit)):raise ValueError('Exact original envelope offsets differ')
    result=independent_oracle(candidate_bytes,proof_bytes,model_bytes,original,columns,prefix=prefix,materialized_at=materialized_at)
    for role in ('object_current','edge_current','tombstone'):
        for row in result[role]:
            witness=witnesses[row['source_delivery_id']];row['source_cursor_json']=witness['cursor']
            if 'apply_batch_id'in row:row['apply_batch_id']=witness['batch_id']
    return result
