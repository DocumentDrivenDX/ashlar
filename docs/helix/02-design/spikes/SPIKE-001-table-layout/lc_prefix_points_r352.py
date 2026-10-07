"""Read-only full-field singleton cohort on scoped maintained39.98M-edge snapshot4."""
import json,time,math,hashlib
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from scale_mixed_r219 import Workload
from mixed_changes_r228 import Changes
from second_changes_r318 import SecondChanges
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_lc_prefix_points_r352';assert not O.exists();s=json.loads((B/'out/native/ashlar_lc_prefix_maintain_r350/audited-summary.json').read_text());assert s['state']=='Bounded LC prefix maintenance preserves all39.98M complete carriers';t={'table':s['table'],'id':s['uuid'],'version':s['after_version']};c=BoundedReads(O);a={'state':'running','table':t,'reads':[]}
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
def metrics(reserve=0):
 c.cursor.close();c.cursor=c.connection.cursor()
 for n in range(12):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==11:raise
   time.sleep(2)
 a['costs']={k:sum(v['metrics'].get(k,0) for v in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert a['costs']['read_bytes']+reserve<=40000000000 and a['costs']['write_remote_bytes']==0
 return h
try:
 c.sql('timeout','SET STATEMENT_TIMEOUT=30');assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
 rows=c.sql('detail','DESCRIBE DETAIL '+t['table']);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,rows[0]))['id']==t['id']
 ranges=c.sql('file-ranges',f"SELECT _metadata.file_path,max(_metadata.file_size),count(*),min(lookup_hash),max(lookup_hash) FROM {t['table']} VERSION AS OF {t['version']} GROUP BY _metadata.file_path");assert sum(int(r[2]) for r in ranges)==39980000;a['files']=ranges;a['hash_domain_spans']=[(int(r[4],16)-int(r[3],16))/2**256 for r in ranges]
 w=Workload(8000000,40000000);fields=list(w.carrier('edge',0));cols=','.join('CAST(published_at AS STRING) AS published_at' if k=='published_at' else k for k in fields)
 changes=Changes(8000000,40000000,100000);inverse=pow(104729,-1,40000000)
 # Deliberately stratified correctness/performance cohort, not service-tail sampling.
 keys=[(i+100000)*104729%40000000 for i in [0,10,20,30,1,2,3,4,5,6,7,8,9,11,12,13]]
 for start in range(0,40000000,2500000):
  ordinal=start+int.from_bytes(hashlib.sha256(str(start).encode()).digest()[:8],'big')%2500000
  while ordinal*inverse%40000000<200000:ordinal=start+(ordinal-start+1)%2500000
  keys.append(ordinal)
 assert len(keys)==32 and len(set(keys))==32
 keys=keys+keys
 for i,ordinal in enumerate(keys):
  if i%4==0:metrics(6500000000)
  row=w.carrier('edge',ordinal);index=ordinal*inverse%40000000;after=SecondChanges().change(index-100000)['after'] if 100000<=index<200000 else changes.change(index)['after'] if index<100000 else row;expected=None if after is None else [None if after[k] is None else after[k].replace('T',' ').removesuffix('Z') if k=='published_at' else after[k] for k in fields]
  actual=c.sql('point-'+str(i),f"SELECT {cols} FROM {t['table']} VERSION AS OF {t['version']} WHERE lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:type AS BIGINT) AND id=CAST(:id AS BIGINT)",parameters={'hash':row['lookup_hash'],'source':row['source_system'],'type':row['rel_type_id'],'id':row['id']});assert actual==([] if expected is None else [expected]);a['reads'].append({'ordinal':ordinal,'id':row['id'],'pass':i//32,'kind':'delete' if after is None else 'update' if index<200000 else 'unchanged','statement_id':c.records[-1]['statement_id']})
 h=metrics();rs=[r for r in c.records if r['label'].startswith('point-')];assert len(rs)==64 and all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in rs);p95=lambda xs:sorted(xs)[math.ceil(.95*len(xs))-1];a['point_metrics']={'caller_p95_ms':p95([r['wall_ms'] for r in rs]),**{k+'_p95':p95([h[r['statement_id']]['metrics'].get(k,0) for r in rs]) for k in ['execution_time_ms','compilation_time_ms','read_bytes','read_files_count']},'remote_queries':sum(h[r['statement_id']]['metrics'].get('read_remote_bytes',0)>0 for r in rs)};a['per_pass']={str(p):{'caller_p95_ms':p95([r['wall_ms'] for r in rs[p*32:(p+1)*32]]),'engine_p95_ms':p95([h[r['statement_id']]['metrics'].get('execution_time_ms',0) for r in rs[p*32:(p+1)*32]])} for p in range(2)};a['per_kind']={kind:{'queries':len([r for i,r in enumerate(rs) if a['reads'][i]['kind']==kind]),'caller_p95_ms':p95([r['wall_ms'] for i,r in enumerate(rs) if a['reads'][i]['kind']==kind]),'engine_p95_ms':p95([h[r['statement_id']]['metrics'].get('execution_time_ms',0) for i,r in enumerate(rs) if a['reads'][i]['kind']==kind])} for kind in ['delete','update','unchanged']};a['state']='All64 exact post-change points and complete39.98M live-file ranges verified';a['qualification']='Post-second-batch private LC selected maintained version, full20field points/absence and all39.98M live file ranges. Cohort4second-deleted12second-updated16unchanged stratified40M, repeated twice. Bounded prefix FULL maintenance with complete carrier preservation; inherited originalLC files, reconstructed first100k batch then second100k mutation. Small ordered warmed/remote descriptive cohort, not controlled all-cold, robust service p95, causality, complete publication or billion admission.';save();print(json.dumps({'spans':a['hash_domain_spans'],'point_metrics':a['point_metrics'],'costs':a['costs']},indent=2))
except Exception as e:a.update(state='Stopped; inspect existing native handles before more admission',error=str(e));save();raise
finally:c.close()
