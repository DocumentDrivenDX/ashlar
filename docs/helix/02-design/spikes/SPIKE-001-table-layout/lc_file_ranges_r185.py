"""Read-only live-file range diagnosis; not a Delta-statistics oracle."""
import json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_lc_file_ranges_r185'
assert not O.exists(),'Inspect existing IDs'
u=json.loads((B/'out/native/ashlar_bucket_update_r176/audited-summary.json').read_text())['owned']['lc'];T=u['table'];c=BoundedReads(O)
c.sql('timeout','SET STATEMENT_TIMEOUT=90')
a=c.sql('detail','DESCRIBE DETAIL '+T);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,a[0]))['id']==u['id'];assert c.sql('version','DESCRIBE HISTORY '+T+' LIMIT 1')[0][0]=='1'
files={};hist={};budget=10000000000
for v in [0,1]:
 used=sum(x['metrics'].get('read_bytes',0) for x in hist.values());assert budget-used>=5000000000,'Reserve 5GB before next full narrow group'
 files[str(v)]=c.sql('ranges-'+str(v),f"SELECT _metadata.file_path, max(_metadata.file_size),count(*),min(lookup_hash),max(lookup_hash),sum(CASE WHEN entity_version=16 AND apply_batch_id='r176-b1' THEN 1 ELSE 0 END) FROM {T} VERSION AS OF {v} GROUP BY _metadata.file_path")
 assert sum(int(x[2]) for x in files[str(v)])==20000000
 c.cursor.close();c.cursor=c.connection.cursor()
 for attempt in range(8):
  hist={x['query_id']:x for x in c.history()}
  if all(r['statement_id'] in hist and hist[r['statement_id']]['is_final'] for r in c.records):break
  time.sleep(2)
 assert all(hist[r['statement_id']]['is_final'] and hist[r['statement_id']]['status']=='FINISHED' for r in c.records)
c.close()
assert sum(int(x[5]) for x in files['0'])==0 and sum(int(x[5]) for x in files['1'])==100000
old=[json.loads(x) for x in (B/'out/native/ashlar_bucket_post_update_reads_r181/statements.jsonl').read_text().splitlines()];keys=next(x for x in old if x['label']=='oracle')['response']['result']['data_array']
newpaths={x[0] for x in files['1']} - {x[0] for x in files['0']}
assert all(int(x[5])==int(x[2]) for x in files['1'] if x[0] in newpaths)
points=[]
for row in keys:
 h=row[16];base=[x for x in files['0'] if x[3]<=h<=x[4]];hot=[x for x in files['1'] if x[0] in newpaths and x[3]<=h<=x[4]]
 points.append({'id':row[2],'lookup_hash':h,'pre_update_live_ranges':len(base),'new_hot_live_ranges':len(hot),'base_compressed_bytes':sum(int(x[1]) for x in base),'hot_compressed_bytes':sum(int(x[1]) for x in hot)})
costs={k:sum(x['metrics'].get(k,0) for x in hist.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert costs['read_bytes']<=budget and costs['write_remote_bytes']==0
s={'state':'Both full20M live-file range groups passed','table':T,'id':u['id'],'versions':[0,1],'files':files,'new_paths':sorted(newpaths),'points':points,'costs':costs,'qualification':'Reconstructed full-precision live hash extrema per file, not stored Delta statistics or physical Parquet row-group pruning. Baseline0 plus new paths identifies potential overlap; DV and truncated stored stats can differ. Compressed file sizes are not actual point-read bytes. No rewrite, publication, latency/rate/billion admission.'}
(O/'summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps({'costs':costs,'new_files':len(newpaths),'first_points':points[:3]},indent=2))
