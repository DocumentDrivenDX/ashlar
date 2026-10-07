"""Tiny exact-before controls isolate successor-version and after-identity guards."""
import hashlib,json,time
from pathlib import Path
from persistent_sql import Client
from overlay_sql_r395 import FIELDS
from fingerprint_guard_r496 import merge
B=Path(__file__).resolve().parent

def main():
 out=B/'out/native/fingerprint_semantic_controls_r499';assert not out.exists();c=Client(out,observation_timeout=60,cancel_after=20);start=time.monotonic();table='client_dev.ashlar_entropy_20261006_r86.generated_fingerprint_r491'
 a={'state':'running exact-before semantic refusals','table':table,'expected_uuid':'f47c3de6-e29e-40ca-811d-0e3b1c744bc0','version':4,'code_sha256':{n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['fingerprint_semantic_controls_r499.py','fingerprint_guard_r496.py']},'bounds':{'read_bytes':1000000,'write_remote_bytes':0,'spill_to_disk_bytes':0,'wall_s':90},'controls':[]}
 def objects(label,q):
  rows=c.sql(label,q);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(cols,r)) for r in rows]
 def head(label):return int(objects(label,'DESCRIBE HISTORY '+table+' LIMIT 1')[0]['version'])
 assert objects('detail','DESCRIBE DETAIL '+table)[0]['id']==a['expected_uuid'];assert head('head-before')==4
 before=c.sql('one-carrier-before','SELECT count(*),min(carrier_fingerprint) FROM '+table);assert before[0][0]=='1'
 for label in ['nonadvancing_version','changed_after_identity']:
  fields=list(FIELDS)+[('id+1' if label=='changed_after_identity' and f=='id' else 'entity_version+1' if label=='changed_after_identity' and f=='entity_version' else f)+' AS after_'+f for f in FIELDS]+['false AS is_delete',"'semantic-control' AS change_delivery_id"]
  source='SELECT '+','.join(fields)+' FROM '+table+' VERSION AS OF 4'
  try:c.sql(label,merge(table,source))
  except RuntimeError:
   r=c.records[-1];assert r['response']['status']['state']=='FAILED' and 'ASHLAR_FINGERPRINT_PREDECESSOR_MISMATCH' in json.dumps(r['response']['status'])
  else:raise AssertionError('Semantic corruption accepted')
  assert head(label+'-head')==4;a['controls'].append({'label':label,'statement_id':r['statement_id'],'exact_before_digest_matches':True,'after_guard_atomic_refusal':True})
 assert c.sql('one-carrier-after','SELECT count(*),min(carrier_fingerprint) FROM '+table)==before
 for i in range(30):
  h=c.history()
  if len(h)==len(c.records) and all(q.get('is_final') for q in h):break
  time.sleep(1)
 assert len(h)==len(c.records) and all(q.get('is_final') for q in h);a['costs']={k:sum(q['metrics'].get(k,0) or 0 for q in h) for k in a['bounds'] if k!='wall_s'};a['wall_s']=time.monotonic()-start;assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and a['wall_s']<=90;a['state']='Both exact-before semantic controls refuse atomically at unchanged version4';(out/'summary.json').write_text(json.dumps(a,indent=2)+'\n');(out/'live-statement.json').rename(out/'completed-last-statement.json');print(json.dumps(a,indent=2))
if __name__=='__main__':main()
