"""Read-only plan inspection; never execute the already-applied MERGE."""
import json,hashlib,time
from pathlib import Path
from inline_guard_sql_r421 import guard_merge_relation
from normalized_apply_sql_r276 import mutation_source,pin
from overlay_sql_r395 import FIELDS
from mixed_change_queries_r230 import row_hash_sql
from bounded_reads_r145 import BoundedReads
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent
def main():
 source=B/'out/native/ashlar_fourth_guard_publish_r437/audited-summary.json';pub=json.loads(source.read_text());assert pub['state']=='Integrated private fourth100k guarded publication passes full change custody';raw=pub['inputs']['source_record'];current=pub['inputs']['current_replacement'];edge=pub['tables']['edge_current'];base=pub['base']['edge_current'];s=mutation_source(raw['table'],0,current['table'],0);variants={'default':s,'inner-broadcast':s.replace('SELECT b.*,','SELECT /*+ BROADCAST(a) */ b.*,'),'outer-broadcast':'SELECT /*+ BROADCAST */ * FROM ('+s+') shaped'}
 O=B/'out/native/ashlar_merge_plan_probe_r443';assert not O.exists();O.mkdir();start=time.monotonic();c=BoundedReads(O,socket_timeout=30);a={'state':'explaining guarded merge and pinned wide join','source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'target':edge,'base':base,'inputs':pub['inputs'],'code_sha256':{n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['merge_plan_probe_r443.py','inline_guard_sql_r421.py','normalized_apply_sql_r276.py']},'plans':{},'bounds':{'read_bytes':10000000,'write_remote_bytes':0,'spill_to_disk_bytes':0,'wall_s':180},'qualification':'EXPLAIN only. Current target7 already contains fourth inputs: executing these MERGEs would be a replay and is forbidden. Companion wide-read plans use qualified immutable E6+input0 to expose source/target joins, not measured performance.'}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 save()
 try:
  c.sql('timeout','SET STATEMENT_TIMEOUT=30');assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
  fields=[*FIELDS,*('after_'+f for f in FIELDS),'is_delete'];comparisons=' AND '.join(f'(b.{f} <=> s.{f})' for f in FIELDS);on=' AND '.join('b.'+f+'=s.'+f for f in ['lookup_hash','source_system','rel_type_id','id'])
  for name,relation in variants.items():
   merge=guard_merge_relation(edge['table'],relation);probe=f"SELECT count(*),count_if(b.id IS NULL OR NOT ({comparisons})),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(['s.'+f for f in fields])}))),256) FROM ({relation}) s LEFT JOIN {pin(base['table'],6)} b ON {on}"
   for kind,q in [('merge',merge),('wide-read',probe)]:
    label=kind+'-'+name;rows=c.sql(label,'EXPLAIN FORMATTED '+q);text='\n'.join(str(r[0]) for r in rows)+'\n';(O/(label+'.txt')).write_text(text);a['plans'][label]={'statement_id':c.records[-1]['statement_id'],'sql':q,'sha256':hashlib.sha256(text.encode()).hexdigest(),'bytes':len(text.encode())};save()
  c.cursor.close();c.cursor=c.connection.cursor()
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert all(v<=a['bounds'][k] for k,v in a['costs'].items());a.update(state='Six native guarded-MERGE/wide-read plans captured without executing writes',wall_s=time.monotonic()-start);assert a['wall_s']<180;save();print(json.dumps({'state':a['state'],'costs':a['costs'],'plans':{k:v['bytes'] for k,v in a['plans'].items()}},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same handles; no write or read replay',error=str(e));save();raise
 finally:c.close()
if __name__=='__main__':main()
