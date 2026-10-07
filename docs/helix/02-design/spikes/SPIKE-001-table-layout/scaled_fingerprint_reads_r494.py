"""Uncached persistent-client repeat comparison on r493 immutable private fixture."""
import hashlib,json,re,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from publication_history import collect_history,HistoryPending
from overlay_sql_r395 import FIELDS
from generated_fingerprint_r491 import expression
B=Path(__file__).resolve().parent

def main():
 parent=B/'out/native/scaled_fingerprint_r493/summary.json';p=json.loads(parent.read_text());assert p['parent_parity']==[['2498646','0']] and p['fixture_integrity']==[['2498646','2498646','0']]
 out=B/'out/native/scaled_fingerprint_reads_r494';assert not out.exists();start=time.monotonic();c=BoundedReads(out,socket_timeout=60)
 a={'state':'running uncached comparisons','source_sha256':hashlib.sha256(parent.read_bytes()).hexdigest(),'code_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'bounds':{'read_bytes':20000000000,'write_remote_bytes':0,'spill_to_disk_bytes':1000000000,'wall_s':300},'reads':{},'qualification':'Three rotated pairs on identical immutable2.498646M-row private generated-column fixture and6227 prepared changes. Uncached query results; storage cache/order/compiler variation remain. No MERGE.'}
 def save():(out/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 save()
 try:
  c.sql('timeout','SET STATEMENT_TIMEOUT=60');assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
  rows=c.sql('detail','DESCRIBE DETAIL '+p['table']);columns=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];detail=dict(zip(columns,rows[0]));assert detail['id']==p['table_id'];a['detail']=detail
  target=p['table']+' VERSION AS OF '+str(p['copy_version']);relation=f"SELECT * FROM {p['source']['table']} VERSION AS OF 0 WHERE lookup_hash<'{p['upper_exclusive_lookup_hash']}'"
  on=' AND '.join('b.'+f+'=s.'+f for f in ['lookup_hash','source_system','rel_type_id','id']);full=' AND '.join(f'(b.{f}<=>s.{f})' for f in FIELDS)
  digest=re.sub(r'\b('+'|'.join(FIELDS)+r')\b',lambda m:'s.'+m.group(0),expression())
  queries={'full':f"SELECT count(*),count_if(b.id IS NULL OR NOT ({full})) FROM ({relation}) s LEFT JOIN {target} b ON {on}",'fingerprint':f"SELECT count(*),count_if(b.id IS NULL OR NOT (b.carrier_fingerprint<=>{digest})) FROM ({relation}) s LEFT JOIN {target} b ON {on}"}
  for i,family in enumerate(['full','fingerprint','fingerprint','full','full','fingerprint']):
   label=family+'-'+str(i);result=c.sql(label,queries[family]);assert result==[['6227','0']];a['reads'][label]={'statement_id':c.records[-1]['statement_id'],'caller_ms':c.records[-1]['wall_ms'],'result':result};save()
  for family,q in queries.items():
   result=c.sql('plan-'+family,'EXPLAIN FORMATTED '+q);(out/('plan-'+family+'.txt')).write_text('\n'.join(str(r[0]) for r in result)+'\n')
  c.cursor.close();c.cursor=c.connection.cursor()
  for i in range(30):
   try:h=collect_history(c.w,c.records,out/'shared-history.json');break
   except HistoryPending:
    if i==29:raise
    time.sleep(1)
  for r in a['reads'].values():r['metrics']=h[r['statement_id']]['metrics'];assert not r['metrics'].get('result_from_cache')
  a['costs']={k:sum(q['metrics'].get(k,0) or 0 for q in h.values()) for k in a['bounds'] if k!='wall_s'};a['wall_s']=time.monotonic()-start;assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and a['wall_s']<=300;a['state']='Six uncached full-carrier/fingerprint comparisons agree at6227 changes';save();(out/'inflight-request.json').rename(out/'completed-last-request.json');print(json.dumps({k:a[k] for k in ['state','costs','wall_s']}));c.close()
 except Exception as e:a.update(state='Stopped; inspect same native handles without replay',error=str(e));save();raise
if __name__=='__main__':main()
