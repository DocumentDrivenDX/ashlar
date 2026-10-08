"""Small native commit/rollback/role/source-reader outbox checks in private sandbox."""
import hashlib,json,re,subprocess,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from ashlar.native import SQLResult
from ashlar.outbox import PostgresOutbox
from ashlar.source import jsonl_batches,records_digest
B=ROOT/'docs/helix/02-design/spikes/SPIKE-001-table-layout';OUT=B/'out/native/outbox_20261008';OUT.mkdir(parents=True,exist_ok=True)
C='ashlar-e2e-truss-pg17'
label=subprocess.run(['docker','inspect',C,'--format','{{index .Config.Labels "ashlar.purpose"}}'],check=True,capture_output=True,text=True).stdout.strip()
if label!='end-to-end-development':raise RuntimeError('Refuse unrelated container')
def pg(sql,expected_failure=False):
    result=subprocess.run(['docker','exec','-i',C,'psql','-q','-At','-U','postgres','-d','truss_e2e','-v','ON_ERROR_STOP=1'],input=sql,text=True,capture_output=True)
    with (OUT/'postgres.jsonl').open('a') as f:f.write(json.dumps({'sql':sql,'exit':result.returncode,'stdout':result.stdout,'stderr':result.stderr})+'\n')
    if expected_failure:
        assert result.returncode and result.stderr;return result.stderr
    if result.returncode:raise RuntimeError(result.stderr)
    return result.stdout.strip()
def literal(text):return "convert_from(decode('"+text.encode('utf-8').hex()+"','hex'),'UTF8')"
assert pg('SHOW server_encoding;')=='UTF8'
assert pg("SELECT count(*) FROM pg_namespace WHERE nspname='ashlar_outbox';")=='0'
ddl=(ROOT/'sql/ashlar-outbox/01-postgresql.sql').read_text();pg(ddl)
raw=(ROOT/'examples/end-to-end/source.jsonl').read_bytes()
source=list(jsonl_batches((ROOT/'examples/end-to-end/graph-source.jsonl').read_bytes().splitlines(keepends=True),feed='outbox',epoch='epoch-1'))
blob=lambda batch:batch.begin+b''.join(x.raw for x in batch.records)+batch.commit
def append(identity,payload,rollback=False):
    return pg('BEGIN; SET ROLE ashlar_outbox_writer; SELECT ashlar_outbox.append('+literal(identity)+','+literal(payload.decode('utf-8'))+')::text; '+('ROLLBACK;' if rollback else 'COMMIT;'))
assert append('fixture-tx-1',raw)=='1'
assert append(source[0].batch_id,blob(source[0]))=='2'
assert append(source[1].batch_id,blob(source[1]),rollback=True)=='3'
assert pg('SELECT position::text FROM ashlar_outbox.head WHERE id=1;')=='2'
assert pg('SELECT count(*) FROM ashlar_outbox.batch;')=='2'
assert append('fixture-tx-1',raw)=='1'
assert pg('SELECT position::text FROM ashlar_outbox.head WHERE id=1;')=='2'
lines=raw.splitlines(keepends=True);lines[1]=lines[1].replace(b'"first"',b'"different"');manifest=json.loads(lines[-1]);manifest['records_sha256']=records_digest(lines[1:-1]);lines[-1]=(json.dumps(manifest)+'\n').encode();conflict=b''.join(lines)
error=pg('BEGIN; SET ROLE ashlar_outbox_writer; SELECT ashlar_outbox.append('+literal('fixture-tx-1')+','+literal(conflict.decode())+'); COMMIT;',True)
assert 'OUTBOX_BATCH_CONFLICT' in error
for role,sql in [('ashlar_outbox_writer',"INSERT INTO ashlar_outbox.batch VALUES(99,'bypass','x',decode('00','hex'));"),('ashlar_outbox_writer','UPDATE ashlar_outbox.head SET position=99 WHERE id=1;'),('ashlar_outbox_reader',"SELECT ashlar_outbox.append('unauthorized','x');")]:
    assert 'permission denied' in pg('SET ROLE '+role+'; '+sql,True)
class Transport:
    def query(self,sql,parameters):
        rendered=re.sub(r'(?<!:):([A-Za-z_][A-Za-z_0-9]*)',lambda m:literal(parameters[m.group(1)]),sql)
        values=pg('SET ROLE ashlar_outbox_reader; SELECT COALESCE(json_agg(r),\'[]\'::json)::text FROM ('+rendered+') r;')
        return SQLResult(json.loads(values))
reader=PostgresOutbox(Transport(),feed='outbox',epoch='epoch-1')
transactions=reader.read('0');assert len(transactions)==2
assert [(x.previous,x.position) for x in transactions]==[('0','1'),('1','2')]
assert blob(transactions[0].batch)==raw and blob(transactions[1].batch)==blob(source[0])
assert reader.read('2')==()
assert reader.read('0',limit=1)[0]==transactions[0]
assert reader.read('1')==(transactions[1],)
summary={'state':'passed','container':C,'source_profile':'ashlar-postgresql-outbox/0.1','ddl_sha256':hashlib.sha256(ddl.encode()).hexdigest(),'committed_groups':2,'committed_events':7,'checks':['native append allocations serialize on head through outer transaction','rolled-back pending position/payload invisible and head unchanged','exact repeat retains original position with no progress allocation','different valid source bytes under same identity refuse','ordinary writer direct batch/head DML denied; reader append denied','committed reader pages preserve original bytes and exact native cursor'],'qualification':'Private PostgreSQL17.9 outbox source plus committed-group reader. No Truss-native mutation/feed, schema acceptance, external application write transaction integration or source acknowledgement. Native scalar positions are separate from contained JSONL byte offsets. Single-writer observed; cross-host blocking/fairness/resource/lost-commit qualification remains. Existing bounded local container; no Databricks or scale workload.'}
(OUT/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary,indent=2))
