"""Read-only single hash-range correctness/pruning pair on immutable inputs."""
import json,time
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
from overlay_sql_r395 import FIELDS
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_range_probe_r450';assert not O.exists();O.mkdir();c=Client(O,observation_timeout=120,cancel_after=75);start=time.monotonic();a={'state':'running immutable range pair','bounds':{'read_bytes':80000000000,'write_remote_bytes':0,'spill_to_disk_bytes':1000000000,'wall_s':240},'reads':{}};F='client_dev.ashlar_entropy_20261006_r86';source=F+'.prepared_current_mutation_r445 VERSION AS OF 0';target=F+'.lc_second_edge_current_r337 VERSION AS OF 6';pred="lookup_hash >= '00' AND lookup_hash < '04'";on=' AND '.join('b.'+f+'=s.'+f for f in ['lookup_hash','source_system','rel_type_id','id']);eq=' AND '.join(f'b.{f}<=>s.{f}' for f in FIELDS)
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
save()
try:
 for label,relation in [('explicit-range','(SELECT * FROM '+target+' WHERE '+pred+')'),('join-only',target)]:
  q=f"/* ashlar range_probe_r450 {label} */ SELECT count(*),count_if(b.id IS NULL OR NOT ({eq})) FROM (SELECT * FROM {source} WHERE {pred}) s LEFT JOIN {relation} b ON {on}"
  result=c.sql(label,q);assert int(result[0][0])>0 and result[0][1]=='0';a['reads'][label]={'result':result,'statement_id':c.records[-1]['statement_id'],'caller_ms':c.records[-1]['wall_ms']};save()
 assert a['reads']['explicit-range']['result']==a['reads']['join-only']['result']
 for i in range(25):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if i==24:raise
   time.sleep(2)
 for r in a['reads'].values():r['metrics']=h[r['statement_id']]['metrics'];assert not r['metrics'].get('result_from_cache')
 a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in a['bounds'] if k!='wall_s'};assert all(v<=a['bounds'][k] for k,v in a['costs'].items());a.update(state='Complete20field immutable range pair passes',wall_s=time.monotonic()-start);assert a['wall_s']<240;save();(O/'live-statement.json').rename(O/'completed-last-statement.json');print(json.dumps(a,indent=2))
except Exception as e:a.update(state='Stopped; inspect same handles without replay',error=str(e));save();raise
