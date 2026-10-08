"""Two-row native raw stage check; process-serialized development authority only."""
from contextlib import contextmanager
from dataclasses import asdict
import base64
import fcntl
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1]
B=ROOT/'docs/helix/02-design/spikes/SPIKE-001-table-layout'
sys.path.insert(0,str(ROOT/'src'));sys.path.insert(0,str(B))
from ashlar.staging import DeltaBatchStage,batch_row
from ashlar.source import jsonl_batches,records_digest
from ashlar.native import SQLResult
from persistent_sql import Client
OUT=B/'out/native/source_stage_20261008';c=Client(OUT)
TABLE='client_dev.ashlar_layout_v03_20261006_r73.source_batch_stage_20261008'
class Transport:
    def query(self,sql,parameters):
        values=[{'name':k,'type':'STRING','value':v} for k,v in parameters.items()]
        rows=c.sql('raw-stage',sql,parameters=values or None)
        columns=c.records[-1]['response'].get('manifest',{}).get('schema',{}).get('columns',[])
        names=[x['name'] for x in columns]
        return SQLResult([dict(zip(names,row)) for row in rows])
class DevelopmentPolicy:
    @contextmanager
    def writer(self,table,uuid,context):
        if table!=TABLE or context!='synthetic-local-development':raise RuntimeError('Development context refused')
        # Local cooperating processes only. This is not remote/native caller fencing.
        with open('/private/tmp/ashlar-source-stage.lock','a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            try:yield
            finally:fcntl.flock(lock,fcntl.LOCK_UN)
raw=(ROOT/'examples/end-to-end/source.jsonl').read_bytes()
batch=list(jsonl_batches(raw.splitlines(keepends=True),feed='synthetic-jsonl',epoch='epoch-1'))[0]
fields=list(batch_row(batch))
c.sql('create-raw-batch-stage','CREATE TABLE '+TABLE+' ('+','.join(k+' STRING NOT NULL' for k in fields)+') USING DELTA')
c.sql('batch-digest-check','ALTER TABLE '+TABLE+' ADD CONSTRAINT batch_digest_check CHECK (batch_digest=sha2(batch_json,256))')
# Resolve UUID by the actual returned named field rather than positional assumption.
transport=Transport();uuid=transport.query('DESCRIBE DETAIL '+TABLE,{}).rows[0]['id']
stage=DeltaBatchStage(transport,DevelopmentPolicy(),TABLE,uuid)
receipt=stage.stage(batch,context='synthetic-local-development')
replay=stage.stage(batch,context='synthetic-local-development')
assert replay==receipt
# Independent committed manifest with changed valid source content, same batch identity.
lines=raw.splitlines(keepends=True);lines[1]=lines[1].replace(b'"first"',b'"different"')
commit=json.loads(lines[-1]);commit['records_sha256']=records_digest(lines[1:-1]);lines[-1]=(json.dumps(commit,separators=(',',':'))+'\n').encode()
conflict=list(jsonl_batches(lines,feed=batch.feed,epoch=batch.epoch))[0]
try:
    stage.stage(conflict,context='synthetic-local-development')
    raise AssertionError('Changed original batch accepted')
except RuntimeError as exc:assert 'SOURCE_BATCH_CONFLICT' in str(exc)
assert stage.stage(batch,context='synthetic-local-development')==receipt
stored=transport.query('SELECT batch_json FROM '+TABLE,{}).rows
assert len(stored)==1
payload=json.loads(stored[0]['batch_json'])
recovered=base64.b64decode(payload['begin_base64'])+b''.join(base64.b64decode(e['raw_base64']) for e in payload['records'])+base64.b64decode(payload['commit_base64'])
assert recovered==raw
summary={'state':'passed','stage_receipt':asdict(receipt),'batches':1,'source_events':2,'checks':['actual UC Delta batch append and original exact raw byte custody','identical replay preserves receipt and row','independently valid changed source commit refuses with original row unchanged','table UUID and named-field parity around each stage'],'qualification':'Raw staging only; local cooperating-process exclusion under administrative development authority, not native/remote source fencing or production uniqueness. No event apply, manifest publication, schema acceptance or checkpoint acknowledgement. Existing authorized warehouse; two synthetic events, no benchmark/resize.'}
(OUT/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary,indent=2))
