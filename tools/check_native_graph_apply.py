"""Small native canonical-current/tombstone apply evidence; unpublished fixture only."""
from dataclasses import asdict
import base64,datetime,hashlib,json,re,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];B=ROOT/'docs/helix/02-design/spikes/SPIKE-001-table-layout'
sys.path.insert(0,str(ROOT/'src'));sys.path.insert(0,str(B))
from ashlar.apply import empty_state,plan_apply
from ashlar.source import jsonl_batches
from ashlar.whole_entity import changes_from_batch
from persistent_sql import Client
OUT=B/'out/native/graph_apply_20261008';c=Client(OUT)
N='client_dev.ashlar_graph_apply_20261008'
baseline=(ROOT/'sql/ashlar-delta-v03/01-baseline.sql').read_text()
if '--resume-empty' in sys.argv:
    for table in ['object_current','edge_current','tombstone','whole_source_history']:
        assert c.sql('verify-empty-'+table,'SELECT count(*) FROM '+N+'.'+table)==[['0']]
else:
    c.sql('create-private-schema','CREATE SCHEMA '+N)
    for table in ['object_current','edge_current','tombstone']:
        ddl=re.search(r'CREATE TABLE '+table+r' \(.*?;',baseline,re.S).group(0)
        c.sql('create-'+table,ddl.replace('CREATE TABLE '+table,'CREATE TABLE '+N+'.'+table,1))
    c.sql('create-whole-source-history','CREATE TABLE '+N+'.whole_source_history (feed STRING NOT NULL, epoch STRING NOT NULL, delivery_id STRING NOT NULL, digest STRING NOT NULL, change_json STRING NOT NULL, raw_base64 STRING NOT NULL) USING DELTA')
raw=(ROOT/'examples/end-to-end/graph-source.jsonl').read_bytes()
batches=list(jsonl_batches(raw.splitlines(keepends=True),feed='whole-entity-fixture',epoch='epoch-1'))
state=empty_state()
def admit(change):
    # Independent fixed synthetic profile only; no actual Truss accepted revision.
    if change.state.schema_revision!='fixture-schema-1' or change.state.key.source!='whole-fixture':raise RuntimeError('Unsupported synthetic schema/source')

def insert_rows(table,rows,types):
    if not rows:return
    fields=list(rows[0]);definition='ARRAY<STRUCT<'+','.join(k+':STRING' for k in fields)+'>>'
    casts=','.join('cast(r.'+k+' AS '+types.get(k,'STRING')+') AS '+k for k in fields)
    c.sql('materialize-'+table,'INSERT INTO '+N+'.'+table+' ('+','.join(fields)+") SELECT "+casts+" FROM (SELECT explode(from_json(:rows,'"+definition+"')) r)",parameters=[{'name':'rows','type':'STRING','value':json.dumps(rows)}])

def current_row(entity,change,batch):
    key=entity.key;typed='type_id' if key.kind=='object' else 'rel_type_id'
    identity={'source_system':key.source,typed:key.type_id,'id':key.id}
    hashed=hashlib.sha256(json.dumps(identity,separators=(',',':')).encode()).hexdigest()
    row={k:str(v) for k,v in identity.items()}
    row.update(logical_key_json='[]' if key.kind=='object' else None,schema_revision=entity.schema_revision,entity_version=str(entity.version),props_json=entity.props_json,retained_json=entity.retained_json,
      source_feed=change.feed,source_epoch=change.epoch,source_cursor_json=json.dumps({'profile':batch.profile,'offset':batch.cursor_after}),source_delivery_id=change.delivery_id,
      published_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),lookup_hash=hashed,apply_batch_id=batch.batch_id)
    if key.kind=='edge':
        row.pop('logical_key_json');a,b=entity.endpoints
        row.update(source_type=str(a.type_id),source_id=str(a.id),target_type=str(b.type_id),target_id=str(b.id))
    return row

for ordinal,batch in enumerate(batches):
    changes=changes_from_batch(batch);new=plan_apply(state,changes,schema_policy=admit)
    # Fresh private fixture, process-serialized only. No outside table writer admitted.
    # Replace only fixture rows touched by this completed source transaction.
    for table,kind,typed in [('object_current','object','type_id'),('edge_current','edge','rel_type_id')]:
        touched=[x for x in changes if x.state.key.kind==kind]
        if touched:
            keys=[{'type_id':str(key.type_id),'id':str(key.id)} for key in sorted({x.state.key for x in touched})]
            c.sql('remove-touched-'+table,'MERGE INTO '+N+'.'+table+" t USING (SELECT k.* FROM (SELECT explode(from_json(:keys,'ARRAY<STRUCT<type_id:STRING,id:STRING>>')) k)) s ON t.source_system='whole-fixture' AND t."+typed+"=cast(s.type_id AS BIGINT) AND t.id=cast(s.id AS BIGINT) WHEN MATCHED THEN DELETE",parameters=[{'name':'keys','type':'STRING','value':json.dumps(keys)}])
            rows=[]
            for key in sorted({x.state.key for x in touched}):
                if key in new.current:
                    change=next(x for x in reversed(changes) if x.state.key==key)
                    rows.append(current_row(new.current[key],change,batch))
            insert_rows(table,rows,{k:'BIGINT' for k in ['type_id','rel_type_id','id','entity_version','source_type','source_id','target_type','target_id']}|{'published_at':'TIMESTAMP'})
    history=[{'feed':x.feed,'epoch':x.epoch,'delivery_id':x.delivery_id,'digest':x.raw_digest,'change_json':json.dumps(asdict(x),separators=(',',':')),'raw_base64':base64.b64encode(record.raw).decode()} for x,record in zip(changes,batch.records)]
    insert_rows('whole_source_history',history,{})
    deletes=[{'source_system':x.state.key.source,'entity_kind':x.state.key.kind,'type_id':str(x.state.key.type_id),'id':str(x.state.key.id),'entity_version':str(x.state.version),'source_feed':x.feed,'source_epoch':x.epoch,'source_cursor_json':json.dumps({'offset':batch.cursor_after}),'source_delivery_id':x.delivery_id} for x in changes if x.operation=='delete']
    insert_rows('tombstone',deletes,{k:'BIGINT' for k in ['type_id','id','entity_version']})
    # Full independent tiny-fixture expected inventories; not production full-graph collect.
    objects=c.sql('object-parity','SELECT cast(type_id AS STRING),cast(id AS STRING),cast(entity_version AS STRING),props_json,retained_json FROM '+N+'.object_current ORDER BY type_id,id')
    edges=c.sql('edge-parity','SELECT cast(rel_type_id AS STRING),cast(id AS STRING),cast(source_type AS STRING),cast(source_id AS STRING),cast(target_type AS STRING),cast(target_id AS STRING) FROM '+N+'.edge_current ORDER BY rel_type_id,id')
    if ordinal==0:
        assert objects==[['1','1','1','{"23":"first"}','{}'],['1','2','1','{"23":"second"}','{}'],['1','3','1','{"23":"isolated"}','{}']]
        assert edges==[['2','1','1','1','1','2'],['2','2','1','1','1','2']]
    else:
        assert objects==[['1','1','2','{"23":"updated","24":null}','{"future":18446744073709551615}'],['1','3','1','{"23":"isolated"}','{}']]
        assert edges==[]
    state=new
history=c.sql('original-history','SELECT delivery_id,raw_base64 FROM '+N+'.whole_source_history ORDER BY delivery_id')
expected=sorted([[record.delivery_id,base64.b64encode(record.raw).decode()] for batch in batches for record in batch.records])
assert history==expected
assert c.sql('delete-parity','SELECT entity_kind,cast(type_id AS STRING),cast(id AS STRING),cast(entity_version AS STRING) FROM '+N+'.tombstone ORDER BY entity_kind,id')==[['edge','2','1','2'],['edge','2','2','2'],['object','1','2','2']]
summary={'state':'passed','schema':N,'source_batches':2,'source_events':len(history),'checks':['real streamed complete source batches normalized with explicit whole-entity versions','canonical object/edge current and tombstone DDL','independent create/parallel-edge/isolated-node inventories','property replacement preserves explicit null and wide retained token','delete removes current, preserves three tombstones and all original source event bytes'],'qualification':'Unpublished process-serialized synthetic whole-entity native apply experiment. No accepted Truss schema, native fencing, replay/recovery producer, property_journal reconstruction, immutable manifest or checkpoint. Intermediate table heads are not a read publication. Existing warehouse; nine small source events, no benchmark/resize.'}
(OUT/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary,indent=2))
