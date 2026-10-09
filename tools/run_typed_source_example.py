"""Small authored core0.8 typed ingest through actual UMF checks and DuckDB.

DuckDB screening profile, not Unity Catalog Delta or native Weft qualification.
"""
import argparse,hashlib,json,shutil,subprocess,tempfile
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from ashlar.apply import empty_state,plan_apply
from ashlar.source import jsonl_batches
from ashlar.whole_entity import changes_from_batch
from ashlar.typed_source_policy import TypedSourcePolicy,typed_values,RECORD_CHECK_PIN


def run(umf_source,output):
    output=Path(output)
    if output.exists():raise ValueError('Fresh output directory required')
    output.mkdir(parents=True)
    example=ROOT/'examples/end-to-end';model=(example/'typed-model.umf.json').read_bytes();raw=(example/'typed-source.jsonl').read_bytes()
    binding=json.loads((example/'typed-binding.json').read_text());b=binding['binding'];fields={pid:tuple(ref) for pid,ref in b['fields'].items()};bindings={binding['type_id']:{'record':tuple(b['record']),'fields':fields}}
    with (example/'typed-source.jsonl').open('rb') as source:batches=tuple(jsonl_batches(source,feed='typed-jsonl',epoch='typed-1'))
    changes=[c for batch in batches for c in changes_from_batch(batch)]
    records=[{'deliveryId':c.delivery_id,'recordSha256':c.raw_digest,'typeId':c.state.key.type_id,'propsSha256':hashlib.sha256(c.state.props_json.encode()).hexdigest(),'identity':{'module':b['record'][0],'element':b['record'][1]},'values':typed_values(c.state.props_json,fields)} for c in changes]
    request=output/'request.json';request.write_text(json.dumps({'records':records},ensure_ascii=False,separators=(',',':')))
    checked=subprocess.run(['bun',str(ROOT/'tools/check_typed_umf_records.ts'),str(umf_source),str(example/'typed-model.umf.json'),str(request)],capture_output=True,check=True)
    (output/'umf-record-check.json').write_bytes(checked.stdout)
    policy=TypedSourcePolicy(model,checked.stdout,bindings,source_system=binding['source_system'],schema_revision=binding['document_revision'],trusted_producer_revision=RECORD_CHECK_PIN)
    expected=json.loads((example/'typed-expected.json').read_text());state=empty_state();snapshots=[];sql=[]
    sql.append('CREATE TABLE object_current(source_system VARCHAR,type_id BIGINT,id BIGINT,entity_version BIGINT,schema_revision VARCHAR,props_json VARCHAR,retained_json VARCHAR); CREATE TABLE whole_source_history(delivery_id VARCHAR,raw_digest VARCHAR,raw_base64 VARCHAR); CREATE TABLE tombstone(id BIGINT,entity_version BIGINT); CREATE TABLE source_snapshots(ordinal BIGINT,id BIGINT,props_json VARCHAR);')
    def quote(v):return "'"+v.replace("'","''")+"'"
    import base64
    for ordinal,batch in enumerate(batches,1):
        after=plan_apply(state,changes_from_batch(batch),schema_policy=policy)
        sql.append('BEGIN; DELETE FROM object_current; DELETE FROM tombstone;')
        for key,entity in after.current.items():sql.append('INSERT INTO object_current VALUES ('+','.join([quote(key.source),str(key.type_id),str(key.id),str(entity.version),quote(entity.schema_revision),quote(entity.props_json),quote(entity.retained_json)])+');')
        for key,c in after.tombstones.items():sql.append(f'INSERT INTO tombstone VALUES ({key.id},{c.state.version});')
        for record in batch.records:sql.append('INSERT INTO whole_source_history VALUES ('+','.join(map(quote,[record.delivery_id,record.sha256,base64.b64encode(record.raw).decode()]))+');')
        sql.append(f'INSERT INTO source_snapshots SELECT {ordinal},id,props_json FROM object_current; COMMIT;');state=after;snapshots.append({str(k.id):v.props_json for k,v in state.current.items()})
    replay=plan_apply(state,changes_from_batch(batches[-1]),schema_policy=policy)
    if replay!=state or snapshots!=expected['transaction_snapshots']:raise AssertionError('Independent first-state or replay mismatch')
    current=[{'id':k.id,'entity_version':v.version,'props_json':v.props_json} for k,v in sorted(state.current.items())]
    if current!=expected['current'] or len(state.history)!=expected['history_events'] or sorted(k.id for k in state.tombstones)!=expected['tombstone_ids']:raise AssertionError('Independent source inventory mismatch')
    # Exact typed native projections; raw lexical properties remain independent VARCHAR custody.
    sql.append('''SELECT CAST(id AS VARCHAR) AS id,props_json,retained_json,
CAST(CAST(json_extract_string(props_json,'$."21"') AS UBIGINT) AS VARCHAR) AS quantity,
CAST(CAST(json_extract_string(props_json,'$."22"') AS DECIMAL(18,2)) AS VARCHAR) AS amount,
CAST(json_extract_string(props_json,'$."23"') AS BOOLEAN) AS active,
json_extract_string(props_json,'$."24"') AS note,
(SELECT count(*) FROM whole_source_history) AS history_events,
(SELECT count(*) FROM tombstone) AS tombstones FROM object_current ORDER BY id;''')
    script='\n'.join(sql);(output/'ingest.sql').write_text(script)
    engine=shutil.which('duckdb')
    if not engine:raise RuntimeError('Existing DuckDB CLI required')
    version=subprocess.check_output([engine,'--version'],text=True).strip()
    native=subprocess.run([engine,'-json',str(output/'typed.duckdb')],input=script,text=True,capture_output=True,check=True)
    (output/'native-result.json').write_text(native.stdout);rows=json.loads(native.stdout)
    want=[{'id':'1','props_json':expected['current'][0]['props_json'],'retained_json':expected['retained_json'],'quantity':expected['final_quantity'],'amount':expected['final_amount'],'active':expected['final_active'],'note':expected['final_note'],'history_events':4,'tombstones':1}]
    if rows!=want:raise AssertionError('Native exact typed query differs from independent expected fixture')
    def observe(name,query):
        observed=subprocess.run([engine,'-json',str(output/'typed.duckdb'),query],text=True,capture_output=True,check=True)
        (output/(name+'.json')).write_text(observed.stdout)
        return json.loads(observed.stdout)
    history=observe('history-readback','SELECT delivery_id,raw_digest,raw_base64 FROM whole_source_history ORDER BY delivery_id')
    original_history=sorted([{'delivery_id':record.delivery_id,'raw_digest':record.sha256,'raw_base64':base64.b64encode(record.raw).decode()} for batch in batches for record in batch.records],key=lambda row:row['delivery_id'])
    if history!=original_history:raise AssertionError('Native original history bytes differ')
    native_snapshots=observe('snapshots-readback','SELECT ordinal,id,props_json FROM source_snapshots ORDER BY ordinal,id')
    expected_snapshots=[{'ordinal':ordinal,'id':int(identity),'props_json':props} for ordinal,snapshot in enumerate(expected['transaction_snapshots'],1) for identity,props in sorted(snapshot.items())]
    if native_snapshots!=expected_snapshots:raise AssertionError('Native transaction snapshots differ')
    # Exact replay was admitted by the original public policy and pure planner.
    # The replay effect driver submits no SQL writes, then re-observes native state.
    replay_rows=observe('replay-readback',sql[-1])
    if replay_rows!=rows or observe('replay-history-readback','SELECT delivery_id,raw_digest,raw_base64 FROM whole_source_history ORDER BY delivery_id')!=original_history:
        raise AssertionError('Native replay state/history changed')
    summary={'state':'passed-local-typed-ingest','umf_producer_revision':RECORD_CHECK_PIN,'model_sha256':hashlib.sha256(model).hexdigest(),'source_sha256':hashlib.sha256(raw).hexdigest(),'transactions':len(batches),'events':len(changes),'current':current,'history_events':len(state.history),'tombstones':len(state.tombstones),'replay_unchanged':True,'native_original_history_verified':True,'native_transaction_snapshots_verified':True,'replay_native_writes':0,'duckdb_version':version,'native_rows':rows,'qualification':'Authored core0.8 singleton scalar model, original public UMF Record checks and actual DuckDB screening tables/queries. Explicit development IDs; no Delta, Weft, keys/relationships, native Truss authority, immutable publication or source ACK claim.'}
    (output/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n');return summary

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--umf-source',required=True,type=Path);parser.add_argument('--output',required=True,type=Path);args=parser.parse_args();print(json.dumps(run(args.umf_source,args.output),ensure_ascii=False,indent=2))
