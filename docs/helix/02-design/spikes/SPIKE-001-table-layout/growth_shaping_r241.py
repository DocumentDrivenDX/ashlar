"""Pinned accumulated node file ranges and exact full-row point cohort."""
import json,time,math,hashlib
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from scale_mixed_r219 import Workload
from mixed_change_queries_r230 import row_hash_sql
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_growth_shaping_r241';assert not O.exists();s=json.loads((B/'out/native/ashlar_scale_growth_r239/audited-summary.json').read_text());t=dict(s['tables']['object_current']);t['table']='client_dev.ashlar_entropy_20261006_r86.growth_object_shaped_r241';c=BoundedReads(O);a={'state':'running','table':t,'reads':[]}
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
def metrics(reserve=0):
 c.cursor.close();c.cursor=c.connection.cursor()
 for n in range(12):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==11:raise
   time.sleep(2)
 a['costs']={k:sum(v['metrics'].get(k,0) for v in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert a['costs']['read_bytes']+reserve<=8000000000 and a['costs']['write_remote_bytes']<=1000000000
 return h
try:
 c.sql('timeout','SET STATEMENT_TIMEOUT=180');
 c.sql('clone',f"CREATE TABLE {t['table']} SHALLOW CLONE {s['tables']['object_current']['table']} VERSION AS OF 1")
 c.sql('file-target',f"ALTER TABLE {t['table']} SET TBLPROPERTIES ('delta.targetFileSize'='67108864')")
 c.sql('shape',f"OPTIMIZE {t['table']} FULL")
 hr=c.sql('head','DESCRIBE HISTORY '+t['table']+' LIMIT 1');version=int(hr[0][0]);t['version']=version
 assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
 rows=c.sql('detail','DESCRIBE DETAIL '+t['table']);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];t['id']=dict(zip(names,rows[0]))['id']
 ranges=c.sql('file-ranges',f"SELECT _metadata.file_path,max(_metadata.file_size),count(*),min(lookup_hash),max(lookup_hash) FROM {t['table']} VERSION AS OF {version} GROUP BY _metadata.file_path");assert sum(int(r[2]) for r in ranges)==400000;a['files']=ranges;a['hash_domain_spans']=[(int(r[4],16)-int(r[3],16))/2**256 for r in ranges]
 w=Workload(8000000,40000000);fields=list(w.carrier('node',0));cols=','.join('CAST(published_at AS STRING) AS published_at' if k=='published_at' else k for k in fields)
 expected=s['checks']['object_current'] if 'checks' in s else json.loads((B/'out/native/ashlar_scale_growth_r239/summary.json').read_text())['checks']['object_current']
 actual=c.sql('full-preservation',f"SELECT count(*),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(fields)}))),256) FROM {t['table']} VERSION AS OF {version}");assert actual==[[str(expected['rows']),expected['independent_all_field_digest']]];a['full_preservation']=expected
 keys=[]
 for start in [0,200000]:keys += sorted(range(start,start+200000),key=lambda i:hashlib.sha256(str(i).encode()).digest())[:10]
 # Interleave keys from the two appended origin ranges.
 keys=[v for pair in zip(keys[:10],keys[10:]) for v in pair]
 for i,ordinal in enumerate(keys):
  if i%4==0:metrics(1500000000)
  row=w.carrier('node',ordinal);expected=[None if row[k] is None else row[k].replace('T',' ').removesuffix('Z') if k=='published_at' else row[k] for k in fields]
  actual=c.sql('point-'+str(i),f"SELECT {cols} FROM {t['table']} VERSION AS OF {version} WHERE lookup_hash=:hash AND source_system=:source AND type_id=CAST(:type AS BIGINT) AND id=CAST(:id AS BIGINT)",parameters={'hash':row['lookup_hash'],'source':row['source_system'],'type':row['type_id'],'id':row['id']});assert actual==[expected];a['reads'].append({'ordinal':ordinal,'id':row['id'],'statement_id':c.records[-1]['statement_id']})
 h=metrics();rs=[r for r in c.records if r['label'].startswith('point-')];assert len(rs)==20 and all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in rs);p95=lambda xs:sorted(xs)[math.ceil(.95*len(xs))-1];a['point_metrics']={'caller_p95_ms':p95([r['wall_ms'] for r in rs]),**{k+'_p95':p95([h[r['statement_id']]['metrics'].get(k,0) for r in rs]) for k in ['execution_time_ms','compilation_time_ms','read_bytes','read_files_count']},'remote_queries':sum(h[r['statement_id']]['metrics'].get('read_remote_bytes',0)>0 for r in rs)};a['state']='All20 exact full-field points and complete400k live-file ranges verified';a['qualification']='Fresh owned shallow clone of400k node snapshot1, target64MiB and OPTIMIZE FULL; resulting snapshot pinned, ten SHA-ranked ordinals from each appended range, interleaved; full17fields. File extrema are live values, not stored Delta statistics. No controlled cold/service p95, graph publication, causal clustering or billion admission; metadata scan and prior validation may warm cache. Actual emitted files measured; clone-only maintenance, original baseline preserved. Read cap8GB/write cap1GB;180s SQL timeout, existing compute.';save();print(json.dumps({'spans':a['hash_domain_spans'],'point_metrics':a['point_metrics'],'costs':a['costs']},indent=2))
except Exception as e:a.update(state='Stopped; inspect existing native handles before more admission',error=str(e));save();raise
finally:c.close()
