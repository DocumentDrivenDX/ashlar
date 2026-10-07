"""Private serialized two-role manifest reference; no production pointer or ACK."""
import json,hashlib,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from publication_history import collect_history,HistoryPending
from maintenance_receipt_guard_r359 import check
from scale_mixed_r219 import Workload
from mixed_changes_r228 import Changes
from second_changes_r318 import SecondChanges
B=Path(__file__).resolve().parent

def main():
 O=B/'out/native/ashlar_maintenance_manifest_r360';assert not O.exists(),'Inspect prior handles; no replay'
 source=B/'out/native/ashlar_lc_prefix_maintain_r350/audited-summary.json';a=json.loads(source.read_text());check(a)
 for n,h in a['audit']['source_sha256'].items():assert hashlib.sha256((source.parent/n).read_bytes()).hexdigest()==h
 s=json.loads((B/'out/native/ashlar_lc_prefix_points_r352/audited-summary.json').read_text());E=a['table'];N='client_dev.ashlar_entropy_20261006_r86.growth_object_current_r236';M='client_dev.ashlar_entropy_20261006_r86.maintenance_manifest_r360'
 c=BoundedReads(O,socket_timeout=60);start=time.monotonic();out={'state':'running','source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'table':M,'receipts':[],'reads':[],'bounds':{'read_bytes':10000000000,'write_remote_bytes':10000000,'spill_to_disk_bytes':0,'wall_s':300}}
 def save():(O/'summary.json').write_text(json.dumps(out,indent=2)+'\n')
 def sql(label,q,params=None):
  assert time.monotonic()-start<300
  return c.sql(label,q,parameters=params)
 save()
 try:
  sql('timeout','SET STATEMENT_TIMEOUT=30')
  for label,table,uid in [('edge',E,a['uuid']),('node',N,'ea6b0cbe-28fc-426c-a52b-15844ef14e91')]:
   rows=sql('detail-'+label,'DESCRIBE DETAIL '+table);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(cols,rows[0]))['id']==uid
  sql('create',f'CREATE TABLE {M} (publication_id STRING NOT NULL, descriptor_json STRING NOT NULL, descriptor_sha256 STRING NOT NULL) USING DELTA')
  vector={'object':{'table':N,'uuid':'ea6b0cbe-28fc-426c-a52b-15844ef14e91','version':6},'edge':{'table':E,'uuid':a['uuid'],'version':3}}
  parent={'id':'r360-parent','kind':'reference-parent','parent':None,'progress':{'synthetic-fixture':'second-batch-complete'},'revisions':{'schema':'ashlar-delta/0.3-reference-only'},'vector':vector,'scope':'two-role test, not a complete source/history publication'}
  child=json.loads(json.dumps(parent));child.update(id='r360-maintenance',kind='maintenance',parent=parent['id']);child['vector']['edge']['version']=5;child['maintenance_statement_id']=a['optimize_statement_id'];child['preservation_receipt_sha256']=out['source_sha256']
  def append(label,desc):
   text=json.dumps(desc,separators=(',',':'),sort_keys=True);digest=hashlib.sha256(text.encode()).hexdigest();params={'id':desc['id'],'text':text,'digest':digest}
   prior=sql(label+'-preflight',f'SELECT descriptor_json,descriptor_sha256 FROM {M} WHERE publication_id=:id',{'id':desc['id']})
   if prior and prior!=[[text,digest]]:raise ValueError('conflicting publication ID')
   sql(label+'-merge',f'MERGE INTO {M} t USING (SELECT :id publication_id,:text descriptor_json,:digest descriptor_sha256) s ON t.publication_id=s.publication_id WHEN NOT MATCHED THEN INSERT (publication_id,descriptor_json,descriptor_sha256) VALUES(s.publication_id,s.descriptor_json,s.descriptor_sha256)',params)
   assert sql(label+'-readback',f'SELECT descriptor_json,descriptor_sha256 FROM {M} WHERE publication_id=:id',{'id':desc['id']})==[[text,digest]]
   out['receipts'].append({'label':label,'descriptor':desc,'canonical_json':text,'sha256':digest});save()
  append('parent',parent);append('child',child);append('duplicate-child',child)
  bad=json.loads(json.dumps(child));bad['progress']['synthetic-fixture']='advanced'
  try:append('conflict',bad)
  except ValueError as e:out['conflict_refusal']=str(e)
  else:raise AssertionError('conflicting ID accepted')
  assert sql('manifest-count',f'SELECT count(*),count(DISTINCT publication_id) FROM {M}')==[['2','2']]
  assert parent['progress']==child['progress'] and parent['revisions']==child['revisions'] and parent['vector']['object']==child['vector']['object']
  w=Workload(8000000,40000000);first=Changes(8000000,40000000,100000);second=SecondChanges();inverse=pow(104729,-1,40000000);fields=list(w.carrier('edge',0));cols=','.join('CAST(published_at AS STRING) AS published_at' if f=='published_at' else f for f in fields)
  for i,index in enumerate([0,1,2,3,4,5,16,17]):
   ordinal=s['reads'][index]['ordinal'];row=w.carrier('edge',ordinal);n=ordinal*inverse%40000000;after=second.change(n-100000)['after'] if 100000<=n<200000 else first.change(n)['after'] if n<100000 else row;expected=[] if after is None else [[None if after[f] is None else after[f].replace('T',' ').removesuffix('Z') if f=='published_at' else after[f] for f in fields]]
   for desc in [parent,child]:
    pin=desc['vector']['edge']['version'];label='point-'+desc['id']+'-'+str(i);params={'hash':row['lookup_hash'],'source':row['source_system'],'type':row['rel_type_id'],'id':row['id']};assert sql(label,f'SELECT {cols} FROM {E} VERSION AS OF {pin} WHERE lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:type AS BIGINT) AND id=CAST(:id AS BIGINT)',params)==expected;out['reads'].append({'label':label,'publication_id':desc['id'],'version':pin,'ordinal':ordinal,'statement_id':c.records[-1]['statement_id']});save()
  c.cursor.close();c.cursor=c.connection.cursor()
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  out['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in out['bounds'] if k!='wall_s'};out['wall_s']=time.monotonic()-start;assert all(v<=out['bounds'][k] for k,v in out['costs'].items()) and out['wall_s']<300
  out['state']='Private serialized maintenance successor/replay/conflict and old-pin reads pass';out['qualification']='Two-role reference only; exact20field/absence reads at3/5. Prior full carrier preservation reused. No full raw/history vector, production fencing, concurrent uniqueness, native crash/expiry injection, active-pin registry, source ACK, freshness or billion-scale admission.';save();print(json.dumps({'state':out['state'],'costs':out['costs'],'wall_s':out['wall_s']},indent=2))
 except Exception as e:out.update(state='Stopped; inspect same handles',error=str(e));save();raise
 finally:c.close()
if __name__=='__main__':main()
