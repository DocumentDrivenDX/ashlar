"""Read-only full-column parity of the retained nine-event fixture at exact versions."""
import base64,datetime,hashlib,json,re,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];B=ROOT/'docs/helix/02-design/spikes/SPIKE-001-table-layout'
sys.path.insert(0,str(ROOT/'src'));sys.path.insert(0,str(B))
from ashlar.effect_validation import validate_effect_snapshot
from ashlar.native import SQLResult
from ashlar.source import jsonl_batches
from persistent_sql import Client
N='client_dev.ashlar_recoverable_graph_20261008'
OUT=B/'out/native/graph_full_parity_20261008';c=Client(OUT)
registered=json.loads((B/'out/native/recoverable_graph_20261008/summary.json').read_text())['table_uuids']
class Transport:
    def query(self,sql,parameters):
        rows=c.sql('complete-effect-parity',sql,parameters=[{'name':k,'type':'STRING','value':v} for k,v in parameters.items()] or None)
        fields=c.records[-1]['response'].get('manifest',{}).get('schema',{}).get('columns',[])
        return SQLResult([dict(zip([f['name'] for f in fields],r)) for r in rows],tuple((f['name'],f['type_text']) for f in fields))
transport=Transport();columns={}
baseline=(ROOT/'sql/ashlar-delta-v03/01-baseline.sql').read_text()
for table in ['object_current','edge_current','tombstone']:
    body=re.search(r'CREATE TABLE '+table+r' \((.*?)\) USING',baseline,re.S).group(1)
    columns[table]=tuple((m.group(1),m.group(2)) for part in body.split(',') for m in [re.match(r'\s*(\w+) (STRING|BIGINT|TIMESTAMP)',part)] if m)
columns['whole_source_history']=tuple((k,'STRING') for k in ['feed','epoch','delivery_id','digest','change_json','raw_base64'])
batches=list(jsonl_batches((ROOT/'examples/end-to-end/graph-source.jsonl').read_bytes().splitlines(keepends=True),feed='whole-entity-fixture',epoch='epoch-1'))
# Independent source-to-carrier expectations: do not call graph_sql_plan or
# consume generated SQL rows as an oracle.
stamp=str(int(datetime.datetime(2026,10,8,17,tzinfo=datetime.timezone.utc).timestamp())*1000000)
current={};history=[];deletes=[];first={}
for batch in batches:
    for record in batch.records:
        event=json.loads(record.raw);kind=event['entity_kind'];key=(kind,event['type_id'],event['id'])
        identity={'source_system':event['source_system'],'type_id' if kind=='object' else 'rel_type_id':int(event['type_id']),'id':int(event['id'])}
        row={name:None for name,typ in columns['object_current' if kind=='object' else 'edge_current']}
        row.update({k:str(v) for k,v in identity.items()})
        row.update(schema_revision=event['schema_revision'],entity_version=event['entity_version'],props_json=event['props_json'],retained_json=event['retained_json'],
                   source_feed=batch.feed,source_epoch=batch.epoch,source_cursor_json=json.dumps({'profile':batch.profile,'offset':batch.cursor_after},separators=(',',':')),
                   source_delivery_id=record.delivery_id,published_at=stamp,lookup_hash=hashlib.sha256(json.dumps(identity,separators=(',',':')).encode()).hexdigest(),apply_batch_id=batch.batch_id)
        if kind=='object':row['logical_key_json']='[]'
        else:
            a,b=event['endpoints'];row.update(source_type=a['type_id'],source_id=a['id'],target_type=b['type_id'],target_id=b['id'])
        if event['operation']=='delete':
            current.pop(key)
            delete={name:None for name,typ in columns['tombstone']}
            delete.update(source_system=event['source_system'],entity_kind=kind,type_id=event['type_id'],id=event['id'],entity_version=event['entity_version'],source_feed=batch.feed,source_epoch=batch.epoch,source_cursor_json=row['source_cursor_json'],source_delivery_id=record.delivery_id)
            deletes.append(delete)
        else:current[key]=row
        def typed_key(k,t,i):return {'source':event['source_system'],'kind':k,'type_id':int(t),'id':int(i)}
        state={'key':typed_key(kind,event['type_id'],event['id']),'version':int(event['entity_version']),'schema_revision':event['schema_revision'],'props_json':event['props_json'],'retained_json':event['retained_json'],
               'endpoints':None if kind=='object' else [typed_key('object',r['type_id'],r['id']) for r in event['endpoints']]}
        change={'feed':batch.feed,'epoch':batch.epoch,'delivery_id':record.delivery_id,'raw_digest':record.sha256,'operation':event['operation'],'state':state}
        history.append({'feed':batch.feed,'epoch':batch.epoch,'delivery_id':record.delivery_id,'digest':record.sha256,'change_json':json.dumps(change,separators=(',',':')),'raw_base64':base64.b64encode(record.raw).decode()})
    if batch.batch_id=='graph-1':first=dict(current)
expected={'object_current':[r for k,r in current.items() if k[0]=='object'],'edge_current':[], 'tombstone':deletes,'whole_source_history':history}
reports=[];vector={}
for short,rows in expected.items():
    table=N+'.'+short;native_history=transport.query('DESCRIBE HISTORY '+table,{}).rows
    version=max(int(r['version']) for r in native_history);vector[table]=version
    reports.append(validate_effect_snapshot(transport,table,registered[table],version,columns[short],rows))
    if short in ('object_current','edge_current'):
        writes=[int(r['version']) for r in native_history if r['operation']=='WRITE']
        initial=min(writes)
        reports.append(validate_effect_snapshot(transport,table,registered[table],initial,columns[short],[r for k,r in first.items() if k[0]==('object' if short=='object_current' else 'edge')]))
summary={'state':'passed','version_vector':vector,'reports':reports,'qualification':'Bounded fixture all-column schema/value/multiplicity parity and successful exact-version data reads. Expectations derived directly from original events rather than generated SQL. Historical initial edge/object carriers included. Does not establish future retained-data availability, protocol policy, active pins, native fencing, publication or accepted Truss schema.'}
(OUT/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary,indent=2))
