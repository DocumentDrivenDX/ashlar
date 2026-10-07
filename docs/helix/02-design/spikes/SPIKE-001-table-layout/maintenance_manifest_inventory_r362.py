"""Read-only metadata completion for the owned r360 reference table."""
import json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent

def main():
 source=B/'out/native/ashlar_maintenance_manifest_r360/audited-summary.json';a=json.loads(source.read_text());O=B/'out/native/ashlar_maintenance_manifest_inventory_r362';assert not O.exists();c=BoundedReads(O);start=time.monotonic()
 try:
  rows=c.sql('detail','DESCRIBE DETAIL '+a['table']);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];detail=dict(zip(cols,rows[0]));history=c.sql('history','DESCRIBE HISTORY '+a['table']);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];ledger=[dict(zip(cols,r)) for r in history];version=int(ledger[0]['version']);owned={r['statement_id'] for r in map(json.loads,(source.parent/'statements.jsonl').read_text().splitlines()) if r['label']=='create' or r['label'].endswith('-merge')};assert [int(r['version']) for r in ledger]==list(range(version,-1,-1)) and all(r['queryHistoryStatementId'] in owned for r in ledger)
  rows=c.sql('pinned-manifest',f'SELECT publication_id,descriptor_json,descriptor_sha256 FROM {a["table"]} VERSION AS OF {version} ORDER BY publication_id');expected=sorted([[r['descriptor']['id'],r['canonical_json'],r['sha256']] for r in a['receipts'][:2]]);assert rows==expected
  c.cursor.close();c.cursor=c.connection.cursor()
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  costs={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert costs['read_bytes']<10000000 and costs['write_remote_bytes']==costs['spill_to_disk_bytes']==0
  out={'state':'Owned manifest UUID/complete ledger/pinned two rows verified','table':a['table'],'uuid':detail['id'],'version':version,'detail':detail,'ledger':ledger,'costs':costs,'wall_s':time.monotonic()-start,'source':'out/native/ashlar_maintenance_manifest_r360/audited-summary.json','qualification':'Read-only metadata completion outside original22.602s workload clock; no fence/concurrent uniqueness proof.'};(O/'summary.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({'state':out['state'],'version':version,'costs':costs}))
 finally:c.close()
if __name__=='__main__':main()
