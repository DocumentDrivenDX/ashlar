"""Read-only full100k predecessor and prepared mutation-source qualification."""
import json,time,hashlib
from pathlib import Path
from normalized_apply_sql_r276 import predecessor_check,mutation_source
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent

def main():
 O=B/'out/native/ashlar_second_predecessor_r325';assert not O.exists();O.mkdir();start=time.monotonic()
 paths={'inputs':'out/native/ashlar_second_delta_stage_r324/audited-summary.json','base':'out/native/ashlar_range32_maintain_r316/audited-summary.json'};sources={k:json.loads((B/p).read_text()) for k,p in paths.items()};inputs=sources['inputs']['tables'];base=sources['base'];raw=inputs['source_record'];current=inputs['current_replacement'];edge={'table':base['table'],'id':base['uuid'],'version':base['version']};c=Client(O,observation_timeout=200,cancel_after=180)
 a={'state':'Checking every selected predecessor field','source_sha256':{k:hashlib.sha256((B/p).read_bytes()).hexdigest() for k,p in paths.items()},'pins':{'raw':raw,'current':current,'edge':edge},'bounds':{'read_bytes':150000000000,'write_remote_bytes':0,'spill_to_disk_bytes':20000000000,'wall_s':600,'statement_s':180},'checks':{}}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 save()
 try:
  for role,t in [('raw',raw),('current',current),('edge',edge)]:
   d=c.sql('detail-'+role,'DESCRIBE DETAIL '+t['table']);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,d[0]))['id']==t['id']
  assert c.sql('all-predecessor-fields',predecessor_check(raw['table'],raw['version'],edge['table'],edge['version']))==[['100000','0']];a['checks']['predecessors']='All20 exact fields match100k qualified baseline edges';save()
  q=mutation_source(raw['table'],raw['version'],current['table'],current['version'])
  assert c.sql('mutation-partition',f"SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),count_if(is_delete),count_if(NOT is_delete),count_if(entity_version IS NULL OR entity_version<>1 OR apply_batch_id<>'mixed-bootstrap'),count_if(NOT is_delete AND (after_entity_version IS NULL OR after_entity_version<>2 OR NOT (source_system <=> after_source_system) OR NOT (rel_type_id <=> after_rel_type_id) OR NOT (id <=> after_id))) FROM ({q})")==[['100000','100000','10000','90000','0','0']];a['checks']['mutation_partition']='Unique100k source;10k deletions90k version2 updates with preserved identity and version1 bootstrap predecessor';save()
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  a['costs']={k:sum(x['metrics'].get(k,0) for x in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
  for k,v in a['costs'].items():assert v<=a['bounds'][k]
  assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in c.records if r['label'] in ['all-predecessor-fields','mutation-partition']);a['wall_s']=time.monotonic()-start;assert a['wall_s']<600;a['state']='Full100k predecessor and prepared mutation-source checks pass';a['qualification']='Read-only pinned synthetic input/baseline qualification. No publisher mutation, concurrent fencing, source ACK, manifest or latency/SLO admission; uses previously qualified complete input digests and baseline.';save();print(json.dumps({'state':a['state'],'costs':a['costs'],'wall_s':a['wall_s']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same native read-only handles',error=str(e));save();raise
if __name__=='__main__':main()
