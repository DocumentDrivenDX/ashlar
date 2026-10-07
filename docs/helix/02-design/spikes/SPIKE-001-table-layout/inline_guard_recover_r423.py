"""Read-only custody after identical-timestamp control committed a valid batch."""
import json,time,hashlib
from pathlib import Path
from persistent_sql import Client
from mixed_change_queries_r230 import row_hash_sql
from overlay_sql_r395 import FIELDS
B=Path(__file__).resolve().parent
def main():
 prior=B/'out/native/ashlar_inline_guard_controls_r422';old=json.loads((prior/'summary.json').read_text());assert old['error']=='Invalid source was accepted: mismatch-published_at';records=[json.loads(x) for x in (prior/'statements.jsonl').read_text().splitlines()];last=records[-1];assert last['label']=='mismatch-published_at' and last['response']['status']['state']=='SUCCEEDED';assert len(old['controls'])==15
 O=B/'out/native/ashlar_inline_guard_recover_r423';assert not O.exists();O.mkdir();c=Client(O,observation_timeout=60,cancel_after=30);start=time.monotonic();a={'state':'read-only recovery','source_sha256':{n:hashlib.sha256((prior/n).read_bytes()).hexdigest() for n in ['summary.json','statements.jsonl']},'reason':'Timestamp negative used the identical2026-10-07T00:00:00Z baseline value; native valid MERGE committed. No guard weakness demonstrated. No write replay or table reset.'}
 table=old['table'];history=c.sql('actual-history','DESCRIBE HISTORY '+table+' LIMIT 10');assert [r[0] for r in history]==['1','0'];cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];obj=[dict(zip(cols,r)) for r in history];assert obj[0]['queryHistoryStatementId']==last['statement_id'] and obj[1]['queryHistoryStatementId']==old['create_statement_id'];a['history']=obj
 for version,count,digest in [(0,6,old['before_digest']),(1,5,old['after_digest'])]:
  q=f"SELECT count(*),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(FIELDS)}))),256) FROM {table} VERSION AS OF {version}";assert c.sql('complete-pin-'+str(version),q)==[[str(count),digest]]
 original=Client(prior);original.records=records
 for client in [original,c]:
  for i in range(25):
   hh=client.history();qs={q['query_id']:q for q in hh};ids={r['statement_id'] for r in client.records}
   if ids==set(qs) and all(q.get('is_final') for q in qs.values()):break
   if i==24:raise RuntimeError('Inspect same native telemetry handles')
   time.sleep(2)
  assert all(q['status'] in ['FINISHED','FAILED'] for q in qs.values())
 a['costs']={k:sum(q['metrics'].get(k,0) for client in [original,c] for q in json.loads((client.out/'query-history.json').read_text())) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};a.update(state='Original valid timestamp commit1 and preserved pins0/1 audited read-only',native_original_statements=len(records),native_recovery_statements=len(c.records),wall_s=time.monotonic()-start)
 for directory,rs in [(prior,records),(O,c.records)]:
  live=json.loads((directory/'live-statement.json').read_text());assert live['statement_id']==rs[-1]['statement_id'];(directory/'live-statement.json').rename(directory/'completed-last-statement.json')
 (O/'summary.json').write_text(json.dumps(a,indent=2)+'\n');print(json.dumps(a,indent=2))
if __name__=='__main__':main()
