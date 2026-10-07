"""Finalize saved native IDs and perform isolated untouched custody; no write replay."""
import json,math
from pathlib import Path
from persistent_sql import Client
from bounded_reads_r145 import BoundedReads
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_queue_r157'
s=json.loads((O/'summary.json').read_text())
assert s['state'].startswith('completed one isolated') and len(s['batches'])==1
Q=O/'custody';assert not (Q/'statements.jsonl').exists(),'Inspect prior handle; no re-execution'
c=BoundedReads(Q);c.sql('timeout','SET STATEMENT_TIMEOUT=90')
E='client_dev.ashlar_entropy_20261006_r86.edge_current'
A=s['owned_tables'][0];last=s['batches'][-1];v=last['versions'][A];stage=last['source_stage']
def untouched(table,version):
 return f"SELECT b.source_system,b.rel_type_id,b.id,b._metadata.file_path,b._metadata.row_index FROM {table} VERSION AS OF {version} b LEFT ANTI JOIN {stage} VERSION AS OF 0 k ON b.source_system=k.source_system AND b.rel_type_id=k.rel_type_id AND b.id=k.id"
x,y=untouched(E,23),untouched(A,v)
assert c.sql('untouched-custody',f'SELECT count(*) FROM (({x} EXCEPT ALL {y}) UNION ALL ({y} EXCEPT ALL {x}))')==[['0']]
assert c.sql('canonical-anchor','DESCRIBE HISTORY '+E+' LIMIT 1')[0][0]=='23'
owned=s['owned_tables']+[r['source_stage'] for r in s['batches']]
s['owned_identity']={}
for i,t in enumerate(owned):
 rows=c.sql('detail-'+str(i),'DESCRIBE DETAIL '+t)
 names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
 detail=dict(zip(names,rows[0]));version=int(c.sql('version-'+str(i),'DESCRIBE HISTORY '+t+' LIMIT 1')[0][0])
 s['owned_identity'][t]={'id':detail['id'],'version':version,'numFiles':detail['numFiles'],'sizeInBytes':detail['sizeInBytes']}
c.close()
records=[];hist={}
for out in (O,O/'journal-lane',O/'apply-lane',Q):
 client=Client(out);client.records=[json.loads(l) for l in (out/'statements.jsonl').read_text().splitlines()]
 records.extend(client.records);hist.update({q['query_id']:q for q in client.history()})
assert all(r['response']['status']['state']=='SUCCEEDED' and hist[r['statement_id']]['is_final'] and hist[r['statement_id']]['status']=='FINISHED' for r in records),'Inspect same native IDs, do not repeat custody'
s['phase_metrics']={r['label']:{'caller_ms':r['wall_ms'],'metrics':hist[r['statement_id']]['metrics']} for r in records}
s['cost_totals']={'read_bytes':sum(hist[r['statement_id']]['metrics'].get('read_bytes',0) for r in records),'write_remote_bytes':sum(hist[r['statement_id']]['metrics'].get('write_remote_bytes',0) for r in records)}
s['runtime_versions']=sorted({x['engineInfo'] for batch in s['batches'] for x in batch['commits']})
s['uniform_modeled_100k_record_age_p95_s']=sorted([batch['verified_manifest_offset_s']-(batch['first_arrival_offset_s']+(i+.5)*.0001) for batch in s['batches'] for i in range(100000)])[math.ceil(.95*100000)-1]
s['qualification']='One isolated100k synthetic complete pre-staged inputs, modeled10k/s release, no real producer arrivals, service p95/sustained/burst/billion admission. Exact affected20 fields/raw origins/full wire and journal per batch, global20MIDs, owned full structural proof, post20Mstructural parity,19.9M unchanged physical custody assuming stable schema and immutable files. No concurrent readers or maintenance.'
s['state']='One isolated overlapped-write-and-validation publication finalized and19.9M untouched custody passed; canonical E23 unchanged'
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n')
print(json.dumps({'batches':[{k:r[k] for k in ['batch','processing_s','queue_wait_s','oldest_modeled_record_freshness_s','uniform_record_age_p95_s']} for r in s['batches']],'cost_totals':s['cost_totals'],'record_age_p95':s['uniform_modeled_100k_record_age_p95_s']},indent=2))
