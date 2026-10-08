"""Pinned E10 exact live file intervals; no warehouse/layout mutation."""
import json,time,hashlib
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_current_file_ranges_r606';assert not O.exists();O.mkdir();p=B/'out/native/ashlar_seventh_guard_publish_r596/audited-summary.json';pub=json.loads(p.read_text());t=pub['tables']['edge_current'];assert t['version']==10;c=Client(O,observation_timeout=120,cancel_after=60);start=time.monotonic();a={'state':'reading pinned E10 file intervals','source_sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'table':t,'bounds':{'read_bytes':5000000000,'write_remote_bytes':0,'spill_to_disk_bytes':1000000000,'wall_s':180}}
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
save()
try:
 a['files']=c.sql('file-ranges',f"SELECT _metadata.file_path,max(_metadata.file_size),count(*),min(lookup_hash),max(lookup_hash) FROM {t['table']} VERSION AS OF 10 GROUP BY _metadata.file_path ORDER BY _metadata.file_path");assert sum(int(r[2]) for r in a['files'])==39930000 and len({r[0] for r in a['files']})==len(a['files']);assert all(len(r[3])==len(r[4])==64 and r[3]<=r[4] for r in a['files'])
 for i in range(20):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if i==19:raise
   time.sleep(1)
 a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in a['bounds'] if k!='wall_s'};assert all(v<=a['bounds'][k] for k,v in a['costs'].items());a.update(state='Pinned E10 live rows and exact physical file hash intervals measured',wall_s=time.monotonic()-start);assert a['wall_s']<180;save();(O/'live-statement.json').rename(O/'completed-last-statement.json');print(len(a['files']),a['costs'],a['wall_s'])
except Exception as e:a.update(state='Stopped; inspect same handle without replay',error=str(e));save();raise
