"""Read-only full-column parity of the retained UMF-backed four-event fixture at exact versions."""
import argparse,base64,datetime,hashlib,json,re,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];B=ROOT/'docs/helix/02-design/spikes/SPIKE-001-table-layout'
sys.path.insert(0,str(ROOT/'src'));sys.path.insert(0,str(B))
from ashlar.effect_validation import validate_effect_snapshot
from databricks_transport import sql_result
from ashlar.source import jsonl_batches
from persistent_sql import Client
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--source',choices=['local','outbox','csv'],default='local')
parser.add_argument('--materialized-at',default='2026-10-08T17:00:00+00:00',help='Original retained materialization clock; default is historical fixture metadata')
parser.add_argument('--output',help='Fresh output directory for a new observation')
args=parser.parse_args()
N='ashlar_e2e_private_20261008.'+{'local':'runtime','outbox':'runtime_outbox','csv':'runtime_csv'}[args.source]
OUT=B/('out/native/'+{'local':'local_example_full_parity_20261008','outbox':'outbox_delta_full_parity_20261008','csv':'csv_delta_full_parity_20261008'}[args.source])
if args.output:OUT=Path(args.output)
if OUT.exists():raise ValueError('Parity output already exists; retain original evidence and select a fresh output')
c=Client(OUT)
installation=json.loads((B/('out/native/'+{'local':'private_setup_20261008','outbox':'outbox_setup_20261008','csv':'csv_setup_20261008'}[args.source]+'/summary.json')).read_text())
registered={table: value['uuid'] for table,value in installation['tables'].items()}
if installation['namespace']!=N:raise ValueError('Private installation differs')
class Transport:
    def query(self,sql,parameters):
        c.sql('complete-effect-parity',sql,parameters=[{'name':k,'type':'STRING','value':v} for k,v in parameters.items()] or None)
        return sql_result(c.records[-1]['response'])
transport=Transport();columns={}
baseline=(ROOT/'sql/ashlar-delta-v03/01-baseline.sql').read_text()
for table in ['object_current','edge_current','tombstone']:
    body=re.search(r'CREATE TABLE '+table+r' \((.*?)\) USING',baseline,re.S).group(1)
    columns[table]=tuple((m.group(1),m.group(2)) for part in body.split(',') for m in [re.match(r'\s*(\w+) (STRING|BIGINT|TIMESTAMP)',part)] if m)
columns['whole_source_history']=tuple((k,'STRING') for k in ['feed','epoch','delivery_id','digest','change_json','raw_base64'])
source=ROOT/('examples/end-to-end/schema-evolution-source.jsonl' if args.source=='outbox' else 'examples/end-to-end/local-string-source.jsonl')
feed='native-evolution' if args.source=='outbox' else 'local-jsonl'
if args.source=='csv':
    # Independent CSV oracle: no csv_batches, apply planner or generated SQL.
    import csv
    from ashlar.source import records_digest
    source=ROOT/'examples/end-to-end/string-source.csv';feed='csv-example'
    originals=source.read_bytes().splitlines(keepends=True)
    header=next(csv.reader([originals[0].decode()],strict=True));batches=[]
    def encode(value):return (json.dumps(value,ensure_ascii=False,separators=(',',':'))+'\n').encode()
    for ordinal,raw in enumerate(originals[1:],1):
        row=dict(zip(header,next(csv.reader([raw.decode()],strict=True))))
        custody={'profile':'ashlar-single-line-csv/0.1','header_base64':base64.b64encode(originals[0]).decode(),'row_base64':base64.b64encode(raw).decode(),'row_ordinal':str(ordinal)}
        event={'kind':'event','source_profile':'ashlar-whole-entity/0.1','source_system':'local-example','schema_revision':'3',
            'delivery_id':json.dumps(custody,separators=(',',':')),'entity_kind':'object','type_id':'17','id':row['id'],'entity_version':row['entity_version'],'operation':row['operation'],
            'props_json':json.dumps({'23':row['label'],'24':row['caption']},ensure_ascii=False,separators=(',',':')),
            'retained_json':json.dumps({'source_profile':'ashlar-single-line-csv/0.1','unmapped_columns':{'future':row['future']}},ensure_ascii=False,separators=(',',':'))}
        record=encode(event);ident='csv-row-'+str(ordinal)
        transaction=[encode({'kind':'begin','batch_id':ident}),record,encode({'kind':'commit','batch_id':ident,'record_count':1,'records_sha256':records_digest([record])})]
        batches.extend(jsonl_batches(transaction,feed=feed,epoch='immutable-example-1'))
else:
    batches=list(jsonl_batches(source.read_bytes().splitlines(keepends=True),feed=feed,epoch='example-1'))
if args.source=='outbox':
    # PG contains independent exact transaction blobs; their inner offsets reset
    # to zero. Native group positions are separate retained source intent.
    batches=[tuple(jsonl_batches((batch.begin+b''.join(record.raw for record in batch.records)+batch.commit).splitlines(keepends=True),feed=feed,epoch='example-1'))[0] for batch in batches]
# Independent source-to-carrier expectations: do not call graph_sql_plan or
# consume generated SQL rows as an oracle.
instant=datetime.datetime.fromisoformat(args.materialized_at)
if instant.tzinfo is None or instant.utcoffset()!=datetime.timedelta(0):raise ValueError('Explicit original UTC materialization clock required')
elapsed=instant-datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc)
stamp=str((elapsed.days*86400+elapsed.seconds)*1000000+elapsed.microseconds)
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
    if batch.batch_id in ('local-1','evolution-1','csv-row-1'):first=dict(current)
expected={'object_current':[r for k,r in current.items() if k[0]=='object'],'edge_current':[], 'tombstone':deletes,'whole_source_history':history}
reports=[];vector={}
for short,rows in expected.items():
    table=N+'.'+short;native_history=transport.query('DESCRIBE HISTORY '+table,{}).rows
    version=max(int(r['version']) for r in native_history);vector[table]=version
    reports.append(validate_effect_snapshot(transport,table,registered[table],version,columns[short],rows))
    if short=='object_current':
        writes=[int(r['version']) for r in native_history if r['operation']=='WRITE']
        initial=min(writes)
        reports.append(validate_effect_snapshot(transport,table,registered[table],initial,columns[short],[r for k,r in first.items() if k[0]==('object' if short=='object_current' else 'edge')]))
summary={'state':'passed','source':args.source,'materialized_at':args.materialized_at,'original_source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'version_vector':vector,'reports':reports,'qualification':'Bounded fixture all-column schema/value/multiplicity parity and successful exact-version data reads. Expectations derived directly from original events rather than generated SQL. Historical initial object carrier included; initial edge table remains empty. Does not establish future retained-data availability, protocol policy, active pins, native fencing, publication or accepted Truss schema.'}
(OUT/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print('Validated five exact-version full-column inventories; no publication or acknowledgement')
