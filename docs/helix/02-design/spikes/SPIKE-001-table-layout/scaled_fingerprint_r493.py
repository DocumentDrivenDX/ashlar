"""Private 1/16 parent-copy full-carrier versus enforced-fingerprint comparison."""
import hashlib,json,time
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
from overlay_sql_r395 import FIELDS
from mixed_change_queries_r230 import INTS,TIMES
from generated_fingerprint_r491 import expression,PROFILE
B=Path(__file__).resolve().parent
F='client_dev.ashlar_entropy_20261006_r86'
BOUND='1'+'0'*63

def main():
 out=B/'out/native/scaled_fingerprint_r493';assert not out.exists()
 c=Client(out,observation_timeout=140,cancel_after=110);started=time.monotonic()
 table=F+'.scaled_fingerprint_r493';base=F+'.lc_second_edge_current_r337';source=F+'.prepared_current_mutation_r445'
 expected={'base':'2d2672c4-8618-4b10-af9c-477d2c1fcc80','source':'67829d40-cd20-4068-85f5-c4e8cc578d48'}
 a={'state':'running private scaled comparison','table':table,'base':{'table':base,'version':6},'source':{'table':source,'version':0},'bounds':{'read_bytes':30000000000,'write_remote_bytes':3000000000,'spill_to_disk_bytes':1000000000,'wall_s':600},'fingerprint_profile':PROFILE,'upper_exclusive_lookup_hash':BOUND,'code_sha256':{n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['scaled_fingerprint_r493.py','generated_fingerprint_r491.py']},'reads':{},'qualification':'One private1/16 hash slice of accepted E6, with exact fourth-input predecessor checks. Copy/full parity costs separate from read comparisons. No MERGE/publication/billion-scale or external-reader admission.'}
 def save():(out/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def objects(label,q):
  rows=c.sql(label,q);cols=[v['name'] for v in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(cols,r)) for r in rows]
 def audit():
  for i in range(30):
   try:h=collect_history(c.w,c.records,out/'shared-history.json');break
   except HistoryPending:
    if i==29:raise
    time.sleep(1)
  costs={k:sum(q['metrics'].get(k,0) or 0 for q in h.values()) for k in a['bounds'] if k!='wall_s'}
  a['costs']=costs;a['wall_s']=time.monotonic()-started;save()
  assert all(v<=a['bounds'][k] for k,v in costs.items()) and a['wall_s']<=600
  return h
 save()
 try:
  for key,t in [('base',base),('source',source)]:
   d=objects(key+'-detail','DESCRIBE DETAIL '+t)[0];assert d['id']==expected[key];a[key]['id']=d['id']
   # Immutable versions remain deliberate even though canonical heads are newer.
  cols=','.join(f+(' BIGINT' if f in INTS else ' TIMESTAMP' if f in TIMES else ' STRING') for f in FIELDS)
  c.sql('create',f"CREATE TABLE {table} ({cols},carrier_fingerprint STRING GENERATED ALWAYS AS ({expression()})) USING DELTA CLUSTER BY (lookup_hash) TBLPROPERTIES ('delta.targetFileSize'='67108864','delta.parquet.compression.codec'='zstd')")
  c.sql('disable-predictive','ALTER TABLE '+table+' DISABLE PREDICTIVE OPTIMIZATION')
  c.sql('copy',f"INSERT INTO {table} ({','.join(FIELDS)}) SELECT {','.join(FIELDS)} FROM {base} VERSION AS OF 6 WHERE lookup_hash<'{BOUND}'")
  sid=c.records[-1]['statement_id'];hist=objects('copy-history','DESCRIBE HISTORY '+table+' LIMIT 10');copy=[r for r in hist if r.get('queryHistoryStatementId')==sid];assert len(copy)==1;version=int(copy[0]['version']);a['copy_version']=version;a['copy_statement_id']=sid;a['detail']=objects('fixture-detail','DESCRIBE DETAIL '+table)[0];a['table_id']=a['detail']['id'];audit()
  target=table+' VERSION AS OF '+str(version)
  # Full coverage source-left join, exact20 fields and independently recomputed generation.
  on=' AND '.join('b.'+f+'=t.'+f for f in ['lookup_hash','source_system','rel_type_id','id'])
  full=' AND '.join(f'(b.{f}<=>t.{f})' for f in FIELDS)
  result=c.sql('full-parent-parity',f"SELECT count(*),count_if(t.id IS NULL OR NOT ({full})) FROM (SELECT * FROM {base} VERSION AS OF 6 WHERE lookup_hash<'{BOUND}') b LEFT JOIN {target} t ON {on}");count=int(result[0][0]);assert result==[[str(count),'0']] and 2000000<count<3000000;a['parent_parity']=result;audit()
  result=c.sql('fixture-integrity',f"SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),count_if(NOT (carrier_fingerprint<=>{expression()})) FROM {target}");assert result==[[str(count),str(count),'0']];a['fixture_integrity']=result;audit()
  relation=f"SELECT * FROM {source} VERSION AS OF 0 WHERE lookup_hash<'{BOUND}'"
  on=' AND '.join('b.'+f+'=s.'+f for f in ['lookup_hash','source_system','rel_type_id','id'])
  full=' AND '.join(f'(b.{f}<=>s.{f})' for f in FIELDS)
  # Qualify every source before-value and preserve identical identity join/source relation.
  digest=expression()
  import re
  digest=re.sub(r'\b('+'|'.join(FIELDS)+r')\b',lambda m:'s.'+m.group(0),digest)
  queries={'full':f"SELECT count(*),count_if(b.id IS NULL OR NOT ({full})) FROM ({relation}) s LEFT JOIN {target} b ON {on}",'fingerprint':f"SELECT count(*),count_if(b.id IS NULL OR NOT (b.carrier_fingerprint<=>{digest})) FROM ({relation}) s LEFT JOIN {target} b ON {on}"}
  expected_result=None
  for i,family in enumerate(['full','fingerprint','fingerprint','full','full','fingerprint']):
   label=family+'-'+str(i);r=c.sql(label,queries[family]);assert len(r)==1 and 5000<int(r[0][0])<7500 and r[0][1]=='0'
   if expected_result is None:expected_result=r
   assert r==expected_result;a['reads'][label]={'statement_id':c.records[-1]['statement_id'],'caller_ms':c.records[-1]['wall_ms'],'result':r};h=audit();assert not h[c.records[-1]['statement_id']]['metrics'].get('result_from_cache')
  for family,q in queries.items():
   rows=c.sql('plan-'+family,'EXPLAIN FORMATTED '+q);(out/('plan-'+family+'.txt')).write_text('\n'.join(str(r[0]) for r in rows)+'\n')
  h=audit()
  for r in a['reads'].values():r['metrics']=h[r['statement_id']]['metrics']
  for label in ['copy','full-parent-parity','fixture-integrity']:
   rec=next(r for r in c.records if r['label']==label);a[label+'_metrics']=h[rec['statement_id']]['metrics']
  a['state']='Private scaled full-carrier and enforced-fingerprint checks agree; performance comparison complete';save();(out/'live-statement.json').unlink(missing_ok=True);print(json.dumps({k:a[k] for k in ['state','parent_parity','fixture_integrity','costs','wall_s']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect existing durable same handles; no replay',error=str(e));save();raise
if __name__=='__main__':main()
