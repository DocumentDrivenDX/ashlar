"""Explicit recovery after independently finalized failed race handles."""
import json,time,hashlib
from pathlib import Path
from bounded_reads_r145 import BoundedReads
B=Path(__file__).resolve().parent

def main():
 source=B/'out/native/ashlar_manifest_creation_race_r365/audited-summary.json';a=json.loads(source.read_text());history=json.loads((source.parent/'query-history.json').read_text());prior={q['query_id']:q for q in history};records=json.loads((source.parent/'all-statements.json').read_text());by={r['statement_id']:r for r in records};O=B/'out/native/ashlar_manifest_race_recovery_r367';assert not O.exists();c=BoundedReads(O,socket_timeout=60);start=time.monotonic();out={'state':'running','source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'controls':[],'bounds':{'read_bytes':20000000,'write_remote_bytes':20000000,'spill_to_disk_bytes':0,'wall_s':180}}
 def save():(O/'summary.json').write_text(json.dumps(out,indent=2)+'\n')
 def sql(label,q,params=None):assert time.monotonic()-start<180;return c.sql(label,q,parameters=params)
 def ledger(label,T):
  rows=sql(label,'DESCRIBE HISTORY '+T);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(cols,r)) for r in rows]
 save()
 try:
  sql('timeout','SET STATEMENT_TIMEOUT=30')
  for pair in a['pairs']:
   kind,T=pair['kind'],pair['table'];failed=next(w for w in pair['workers'] if w['state']=='FAILED');sid=failed['statement_id'];assert prior[sid]['status']=='FAILED' and prior[sid]['is_final'] and 'DELTA_CONCURRENT_APPEND.ROW_LEVEL_CHANGES' in failed['error']['message'];r=by[sid]
   rows=sql(kind+'-detail','DESCRIBE DETAIL '+T);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(cols,rows[0]))['id']==pair['uuid'];before=ledger(kind+'-before',T);assert int(before[0]['version'])==int(pair['after_history'][0]['version']);assert sql(kind+'-winner',f'SELECT * FROM {T} ORDER BY publication_id,descriptor_json')==pair['rows']
   control={'kind':kind,'old_failed_statement_id':sid,'table':T,'uuid':pair['uuid'],'before_history':before};out['controls'].append(control);save()
   try:sql(kind+'-recovery',r['sql'],r['parameters']);control['result']='exact replay succeeds';assert kind=='identical'
   except RuntimeError:
    latest=c.records[-1];assert kind=='conflicting' and latest['response']['status']['state']=='FAILED' and 'ASHLAR_PUBLICATION_ID_CONFLICT' in latest['response']['status']['error']['message'];control['result']='native semantic conflict refused'
   control['statement_id']=c.records[-1]['statement_id'];assert sql(kind+'-readback',f'SELECT * FROM {T} ORDER BY publication_id,descriptor_json')==pair['rows'];control['after_history']=ledger(kind+'-after',T)
   if kind=='conflicting':assert control['after_history'][0]['version']==before[0]['version']
   save()
  c.cursor.close();c.cursor=c.connection.cursor()
  for i in range(20):
   history=c.history();native={q['query_id']:q for q in history}
   if set(native)=={r['statement_id'] for r in c.records} and all(q.get('is_final') for q in native.values()):break
   if i==19:raise RuntimeError('Native evidence pending; inspect same IDs')
   time.sleep(2)
  out['costs']={k:sum(q['metrics'].get(k,0) for q in native.values()) for k in out['bounds'] if k!='wall_s'};out['wall_s']=time.monotonic()-start;assert all(v<=out['bounds'][k] for k,v in out['costs'].items()) and out['wall_s']<180;out['state']='Finalized race failures recover to exact duplicate or semantic refusal';out['qualification']='Explicit single recovery after terminal native failure, UUID/head/winner readback. No unknown-outcome replay, auto-retry loop, generic uniqueness/fence, full-role publication, service performance or scale admission.';save();print(json.dumps({'state':out['state'],'costs':out['costs'],'wall_s':out['wall_s']}))
 except Exception as e:out.update(state='Stopped; inspect same handles',error=str(e));save();raise
 finally:c.close()
if __name__=='__main__':main()
