"""Full39.98M twenty-field bridge from qualified range32 vector to LC5."""
import hashlib,json,time
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
from mixed_change_queries_r230 import row_hash_sql
B=Path(__file__).resolve().parent

def main():
 paths={'publication':'out/native/ashlar_incremental_publish_r332/audited-summary.json','lc':'out/native/ashlar_lc_prefix_maintain_r350/audited-summary.json','oracle':'out/second-cdf-image-oracle-r327.json'};src={k:json.loads((B/n).read_text()) for k,n in paths.items()};s=src['publication'];lc=src['lc'];E=s['tables']['edge_current'];O=B/'out/native/ashlar_lc_full_vector_r376';assert not O.exists(),'Inspect prior handles; no blind replay';c=Client(O,observation_timeout=200,cancel_after=120);start=time.monotonic();out={'state':'running','source_sha256':{k:hashlib.sha256((B/n).read_bytes()).hexdigest() for k,n in paths.items()},'sources':paths,'comparison_edge':E,'lc_edge':{'table':lc['table'],'id':lc['uuid'],'version':lc['after_version']},'groups':[],'bounds':{'read_bytes':180000000000,'write_remote_bytes':1000000,'spill_to_disk_bytes':5000000000,'wall_s':900,'statement_cancel_after_s':120}}
 def save():(O/'summary.json').write_text(json.dumps(out,indent=2)+'\n')
 def sql(label,q):assert time.monotonic()-start<900;return c.sql(label,'/* ashlar '+O.name+' '+label+' */ '+q)
 def metrics(reserve=0):
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  out['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert out['costs']['read_bytes']+reserve<=out['bounds']['read_bytes'];assert all(v<=out['bounds'][k] for k,v in out['costs'].items());assert time.monotonic()-start<900;return h
 def detail(label,table):
  rows=sql(label,'DESCRIBE DETAIL '+table);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return dict(zip(cols,rows[0]))
 save()
 try:
  out['range_detail']=detail('range-detail',E['table']);assert out['range_detail']['id']==E['id'];out['lc_detail']=detail('lc-detail',lc['table']);assert out['lc_detail']['id']==lc['uuid'];fields=src['oracle']['roles']['edge_current']['fields'];assert len(fields)==20;out['fields']=fields;hs=row_hash_sql(fields)
  for first in range(0,400,100):
   metrics(40000000000);lo=8000001+first*100000;hi=lo+10000000;q=f"SELECT CAST(floor((id-8000001)/100000) AS BIGINT) group_id,count(*),sha2(concat_ws('',sort_array(collect_list({hs}))),256) FROM {E['table']} VERSION AS OF {E['version']} WHERE id>={lo} AND id<{hi} GROUP BY group_id ORDER BY group_id";rows=sql('range-groups-'+str(first),q);assert len(rows)==100 and [int(r[0]) for r in rows]==list(range(first,first+100));assert rows==lc['checks']['after'][first:first+100];out['groups'].extend(rows);save()
  assert len(out['groups'])==400 and sum(int(r[1]) for r in out['groups'])==39980000;out['state']='All39.98M complete edge carriers bridge exactly to LC5';save();metrics()
  vector=json.loads(json.dumps(s['tables']));vector['edge_current']=out['lc_edge'];out['role_pins']=vector
  for role,t in vector.items():
   assert detail('detail-'+role,t['table'])['id']==t['id'];assert sql('available-'+role,f'SELECT count(*) FROM (SELECT 1 FROM {t["table"]} VERSION AS OF {t["version"]} LIMIT 1)')==[['1']]
  manifest=s['manifest'];assert detail('source-manifest-detail',manifest['table'])['id']==manifest['id'];cols='publication_id,profile_version,table_versions_json,source_progress_json,schema_revisions_json,validation_report_json';original=sql('source-descriptor','SELECT '+cols+' FROM '+manifest['table']);assert len(original)==1;original=original[0];out['original_descriptor']=original;assert json.loads(original[2])==s['publication_vector'];values=['lc-vector-r376',original[1],json.dumps({t['table']:t['version'] for t in vector.values()},sort_keys=True,separators=(',',':')),original[3],original[4],json.dumps({'kind':'LC-full-vector-equivalence-reference','original_validation_report_json':original[5],'source_publication_receipt_sha256':out['source_sha256']['publication'],'lc_preservation_receipt_sha256':out['source_sha256']['lc'],'full20field_equivalence_groups':out['groups'],'production_writer_fence':False},sort_keys=True,separators=(',',':'))];out['descriptor_values']=values
  M='client_dev.ashlar_entropy_20261006_r86.lc_full_vector_manifest_r376';out['manifest_table']=M;sql('create-manifest',f'CREATE TABLE {M} (publication_id STRING NOT NULL,profile_version STRING NOT NULL,table_versions_json STRING NOT NULL,source_progress_json STRING NOT NULL,schema_revisions_json STRING NOT NULL,validation_report_json STRING NOT NULL,recorded_at TIMESTAMP NOT NULL) USING DELTA')
  def lit(v):return "decode(unhex('"+v.encode().hex()+"'),'UTF-8')"
  sql('reference-publication',f'INSERT INTO {M} ({cols},recorded_at) SELECT '+','.join(lit(v) for v in values)+',current_timestamp()');out['publish_statement_id']=c.records[-1]['statement_id'];assert sql('descriptor-readback','SELECT '+cols+' FROM '+M)==[values];out['manifest_detail']=detail('manifest-detail',M);rows=sql('manifest-history','DESCRIBE HISTORY '+M);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];out['manifest_history']=[dict(zip(names,r)) for r in rows];h=metrics();assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in c.records if 'groups-' in r['label']);out['wall_s']=time.monotonic()-start;out['state']='LC5 complete carrier equivalence and six-role reference vector pass';out['qualification']='Four100group native reads cover all39.98M complete20field carriers against separately audited LC5 multisets; SHA256 collision assumption. Exact other-role pins/progress/revisions retained from qualified controlled synthetic publication. Private equivalence manifest only, no integrated ingest timing/source ACK/production fencing/retention/sustained/cold/latency/billion admission.';save();print(json.dumps({'state':out['state'],'costs':out['costs'],'wall_s':out['wall_s']}))
 except Exception as e:out.update(state='Stopped; inspect same handles and partial state; no replay',error=str(e),wall_s=time.monotonic()-start);save();raise
if __name__=='__main__':main()
