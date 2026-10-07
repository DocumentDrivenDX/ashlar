"""40M-edge private stale-role injection: old pins exact, unknown commit unpublished."""
import hashlib,json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from scale_mixed_r219 import Workload
B=Path(__file__).resolve().parent

def custody(history,base,selected,approved):
 interval=[c for c in history if base<int(c['version'])<=selected]
 if [int(c['version']) for c in interval]!=list(range(selected,base,-1)):raise ValueError('incomplete selected interval')
 if any(c['queryHistoryStatementId'] not in approved for c in interval):raise ValueError('unapproved role commit')
 return interval

def main():
 source=B/'out/native/ashlar_scale_edges_r274/audited-summary.json';a=json.loads(source.read_text());E=a['tables']['edge_current'];O=B/'out/native/ashlar_stale_role_barrier_r374';assert not O.exists(),'No blind replay';c=BoundedReads(O,socket_timeout=60);T='client_dev.ashlar_entropy_20261006_r86.stale_edge_current_r374';M='client_dev.ashlar_entropy_20261006_r86.stale_manifest_r374';start=time.monotonic();out={'state':'running','source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'parent':E,'table':T,'manifest':M,'ordinal':1234567,'bounds':{'read_bytes':5000000000,'write_remote_bytes':1000000000,'spill_to_disk_bytes':0,'wall_s':300}}
 def save():(O/'summary.json').write_text(json.dumps(out,indent=2)+'\n')
 def sql(label,q,params=None):assert time.monotonic()-start<300;return c.sql(label,q,parameters=params)
 def detail(label,table):
  rows=sql(label,'DESCRIBE DETAIL '+table);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return dict(zip(cols,rows[0]))
 def ledger(label,table):
  rows=sql(label,'DESCRIBE HISTORY '+table);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(cols,r)) for r in rows]
 save()
 try:
  sql('timeout','SET STATEMENT_TIMEOUT=30');assert detail('parent-detail',E['table'])['id']==E['id'];sql('clone',f'CREATE TABLE {T} SHALLOW CLONE {E["table"]} VERSION AS OF {E["version"]}');out['clone_statement_id']=c.records[-1]['statement_id'];out['detail']=detail('role-detail',T);out['before_history']=ledger('role-before',T);assert [int(h['version']) for h in out['before_history']]==[0]
  vector=json.loads(json.dumps(a['tables']));vector['edge_current']={'table':T,'id':out['detail']['id'],'version':0,'rows':40000000};descriptor={'id':'r374-parent','kind':'bootstrap-reference','vector':vector,'source_receipt_sha256':out['source_sha256'],'scope':'five-role qualified bootstrap reference; no new source progress or production pointer'};text=json.dumps(descriptor,sort_keys=True,separators=(',',':'));digest=hashlib.sha256(text.encode()).hexdigest();out['descriptor']=descriptor;out['canonical_descriptor']=text;out['descriptor_sha256']=digest
  sql('create-manifest',f'CREATE TABLE {M} (publication_id STRING NOT NULL,descriptor_json STRING NOT NULL,descriptor_sha256 STRING NOT NULL) USING DELTA');sql('parent-publication',f'INSERT INTO {M} (publication_id,descriptor_json,descriptor_sha256) VALUES (:id,:text,:digest)',{'id':descriptor['id'],'text':text,'digest':digest});out['manifest_detail']=detail('manifest-detail',M);out['manifest_before']=ledger('manifest-before',M)
  w=Workload(8000000,40000000);row=w.carrier('edge',out['ordinal']);fields=list(row);cols=','.join('CAST(published_at AS STRING) AS published_at' if f=='published_at' else f for f in fields);params={'hash':row['lookup_hash'],'source':row['source_system'],'type':row['rel_type_id'],'id':row['id']};predicate='lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:type AS BIGINT) AND id=CAST(:id AS BIGINT)';expected=[[None if row[f] is None else row[f].replace('T',' ').removesuffix('Z') if f=='published_at' else row[f] for f in fields]];query=lambda pin:f'SELECT {cols} FROM {T} VERSION AS OF {pin} WHERE {predicate}'
  assert sql('before-old-pin',query(0),params)==expected
  marker='{"unapproved_writer":"r374","opaque":"not-a-source-batch"}';sql('injected-role-write',f'UPDATE {T} SET retained_json=:marker WHERE {predicate} AND entity_version=1',dict(params,marker=marker));out['injected_statement_id']=c.records[-1]['statement_id'];out['after_role_history']=ledger('role-after',T);selected=int(out['after_role_history'][0]['version']);assert selected==1;out['candidate_version']=selected
  assert sql('after-old-pin',query(0),params)==expected;new=json.loads(json.dumps(expected));new[0][fields.index('retained_json')]=marker;assert sql('contaminated-head',query(1),params)==new;out['marker']=marker
  # The stale writer's actual SID is deliberately not in the admitted writer set.
  approved={out['clone_statement_id']};out['approved_role_statement_ids']=sorted(approved)
  try:custody(out['after_role_history'],0,selected,approved)
  except ValueError as e:out['publication_refusal']=str(e)
  else:raise AssertionError('Unapproved role commit accepted')
  assert out['publication_refusal']=='unapproved role commit';assert sql('unchanged-manifest',f'SELECT * FROM {M}')==[[descriptor['id'],text,digest]];out['manifest_after']=ledger('manifest-after',M);assert out['manifest_before']==out['manifest_after'];save();c.cursor.close();c.cursor=c.connection.cursor()
  for i in range(20):
   h=c.history();native={q['query_id']:q for q in h}
   if set(native)=={r['statement_id'] for r in c.records} and all(q.get('is_final') for q in native.values()):break
   if i==19:raise RuntimeError('Metrics pending; inspect same IDs')
   time.sleep(2)
  out['costs']={k:sum(q['metrics'].get(k,0) for q in native.values()) for k in out['bounds'] if k!='wall_s'};out['wall_s']=time.monotonic()-start;assert all(v<=out['bounds'][k] for k,v in out['costs'].items()) and out['wall_s']<300;out['state']='40M-edge private stale commit refused; parent manifest and exact old carrier preserved';out['qualification']='One native unexplained role UPDATE against private40M clone, sampled full20field carrier equality at0/1, prior full baseline custody reused. Client custody barrier refuses successor before submission. No real worker-termination fence, full post-update sweep, complete production publication, history repair, source ACK, sustained/cold/latency or billion admission.';save();print(json.dumps({'state':out['state'],'costs':out['costs'],'wall_s':out['wall_s']}))
 except Exception as e:out.update(state='Stopped; inspect same handles',error=str(e));save();raise
 finally:c.close()
if __name__=='__main__':main()
