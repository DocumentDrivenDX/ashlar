"""Bounded read-only100k/full-parent comparison; no replay or canonical writes."""
import json,hashlib,time
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
from normalized_apply_sql_r276 import pin,mutation_source
from overlay_sql_r395 import FIELDS
from mixed_change_queries_r230 import row_hash_sql
B=Path(__file__).resolve().parent
def main():
 pp=B/'out/native/ashlar_prepared_mutation_r445/summary.json';prepared=json.loads(pp.read_text());assert prepared['state']=='Complete42field prepared100k cache matches independent source and native plans captured';pub=json.loads((B/'out/native/ashlar_fourth_guard_publish_r437/audited-summary.json').read_text());base=pub['base']['edge_current'];raw=pub['inputs']['source_record'];current=pub['inputs']['current_replacement'];fields=prepared['fields'];on=' AND '.join('b.'+f+'=s.'+f for f in ['lookup_hash','source_system','rel_type_id','id']);comparisons=' AND '.join(f'(b.{f}<=>s.{f})' for f in FIELDS)
 O=B/'out/native/ashlar_prepared_join_compare_r447';assert not O.exists();O.mkdir();start=time.monotonic();c=Client(O,observation_timeout=130,cancel_after=90);a={'state':'comparing prepared-first and legacy-second immutable joins','prepared_source_sha256':hashlib.sha256(pp.read_bytes()).hexdigest(),'base':base,'reads':{},'bounds':{'read_bytes':80000000000,'write_remote_bytes':0,'spill_to_disk_bytes':5000000000,'wall_s':300},'qualification':'One prepared-first/legacy-second read-only pair at E6+fourth input0. Full20field before comparisons and full42field source digest. Fixed order/cache differences prevent causal speed claims; no MERGE or publication is executed.'}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 save()
 try:
  table=prepared['table'];relations={'prepared':'SELECT * FROM '+pin(table['table'],table['version']),'legacy':mutation_source(raw['table'],0,current['table'],0)}
  for family,relation in relations.items():
   q=f"SELECT count(*),count_if(b.id IS NULL OR NOT ({comparisons})),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(['s.'+f for f in fields])}))),256) FROM ({relation}) s LEFT JOIN {pin(base['table'],6)} b ON {on}"
   result=c.sql(family,'/* ashlar prepared_join_compare_r447 '+family+' */ '+q);assert result==[['100000','0',prepared['oracle_digest']]];a['reads'][family]={'statement_id':c.records[-1]['statement_id'],'caller_s':c.records[-1]['wall_ms']/1000,'result':result};save()
  for i in range(25):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==24:raise
    time.sleep(2)
  for family,r in a['reads'].items():r['metrics']=h[r['statement_id']]['metrics'];assert not r['metrics'].get('result_from_cache')
  a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in a['bounds'] if k!='wall_s'};assert all(v<=a['bounds'][k] for k,v in a['costs'].items());a.update(state='Both100k full-before/42field source matches pass in immutable read-only pair',wall_s=time.monotonic()-start);assert a['wall_s']<300;save();print(json.dumps(a,indent=2))
 except Exception as e:a.update(state='Stopped; inspect same durable native handles and costs; no replay',error=str(e));save();raise
if __name__=='__main__':main()
