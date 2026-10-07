"""Owned legacy/write-time CDF comparison; no graph descriptor or source ACK."""
import json,time,hashlib
from pathlib import Path
from normalized_apply_sql_r276 import pin,current_merge,adjacency_merge
from mixed_change_queries_r230 import row_hash_sql
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent

def main():
 paths={'base':'out/native/ashlar_range32_maintain_r316/audited-summary.json','inputs':'out/native/ashlar_second_delta_stage_r324/audited-summary.json','oracle':'out/second-cdf-image-oracle-r327.json','predecessor':'out/native/ashlar_second_predecessor_r325/audited-summary.json'};s={k:json.loads((B/p).read_text()) for k,p in paths.items()};raw=s['inputs']['tables']['source_record'];current=s['inputs']['tables']['current_replacement'];O=B/'out/native/ashlar_range32_second_apply_r328';assert not O.exists();O.mkdir();start=time.monotonic();c=Client(O,observation_timeout=200,cancel_after=180);a={'state':'Preparing private write-time CDF tables','source_sha256':{k:hashlib.sha256((B/p).read_bytes()).hexdigest() for k,p in paths.items()},'bounds':{'read_bytes':100000000000,'write_remote_bytes':50000000000,'spill_to_disk_bytes':5000000000,'wall_s':900,'statement_s':180},'tables':{},'checks':{},'commits':[]}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def detail(table,label):
  r=c.sql('detail-'+label,'DESCRIBE DETAIL '+table);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return dict(zip(names,r[0]))
 def bind(table,sid,label):
  r=c.sql('history-'+label,'DESCRIBE HISTORY '+table+' LIMIT 20');names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];found=[dict(zip(names,x)) for x in r if dict(zip(names,x)).get('queryHistoryStatementId')==sid];assert len(found)==1;a['commits'].append({'label':label,'statement_id':sid,'history':found[0]});save();return int(found[0]['version'])
 def metrics():
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  a['costs']={k:sum(x['metrics'].get(k,0) for x in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert all(v<=a['bounds'][k] for k,v in a['costs'].items());assert time.monotonic()-start<900;save();return h
 save()
 try:
  assert s['predecessor']['state']=='Full100k predecessor and prepared mutation-source checks pass'
  assert s['predecessor']['pins']['edge']=={'table':s['base']['table'],'id':s['base']['uuid'],'version':s['base']['version']}
  for role in ['source_record','current_replacement']:
   t=s['inputs']['tables'][role];assert detail(t['table'],'input-'+role)['id']==t['id']
  for role in ['edge_current']:
   parent={'table':s['base']['table'],'id':s['base']['uuid'],'version':s['base']['version']};assert detail(parent['table'],'parent-'+role)['id']==parent['id'];table='client_dev.ashlar_entropy_20261006_r86.range32_second_'+role+'_r328';c.sql('clone-'+role,f"CREATE TABLE {table} SHALLOW CLONE {pin(parent['table'],parent['version'])}");sid=c.records[-1]['statement_id'];clone_version=bind(table,sid,'clone-'+role);d=detail(table,role);a['tables'][role]={'table':table,'id':d['id'],'base':parent,'clone_version':clone_version};save();c.sql('disable-'+role,'ALTER TABLE '+table+' DISABLE PREDICTIVE OPTIMIZATION');settings=c.sql('maintenance-'+role,'DESCRIBE TABLE EXTENDED '+table);assert [r[1] for r in settings if r[0]=='Predictive Optimization']==['DISABLE'];c.sql('enable-cdf-'+role,"ALTER TABLE "+table+" SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')");sid=c.records[-1]['statement_id'];a['tables'][role]['cdf_enable_version']=bind(table,sid,'enable-'+role);d=detail(table,'enabled-'+role);assert json.loads(d['properties'])['delta.enableChangeDataFeed']=='true';metrics()
   q=(current_merge if role=='edge_current' else adjacency_merge)(table,raw['table'],raw['version'],current['table'],current['version']);q=q.replace('ON b.lookup_hash=s.lookup_hash',"ON b.lookup_partition=CAST(floor(CAST(conv(substr(s.lookup_hash,1,2),16,10) AS BIGINT)/8) AS INT) AND b.lookup_hash=s.lookup_hash");apply_start=time.monotonic();c.sql('merge-'+role,q);sid=c.records[-1]['statement_id'];a['tables'][role]['apply_caller_s']=time.monotonic()-apply_start;version=bind(table,sid,'merge-'+role);a['tables'][role]['version']=version;m=json.loads(a['commits'][-1]['history']['operationMetrics']);assert m['numTargetRowsUpdated']=='90000' and m['numTargetRowsDeleted']=='10000' and m['numTargetRowsInserted']=='0';a['state']='Checking complete legacy CDF images';save();metrics()
   expected=s['oracle']['roles'][role];q=f"SELECT _change_type,count(*),min(_commit_version),max(_commit_version),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(expected['fields'])}))),256) FROM table_changes('{table}',{version},{version}) GROUP BY _change_type ORDER BY _change_type";want=[[kind,str(x['rows']),str(version),str(version),x['digest']] for kind,x in sorted(expected['images'].items())];read_start=time.monotonic();assert c.sql('all-images-'+role,q)==want;a['checks'][role]={'rows':190000,'groups':want,'cdf_read_caller_s':time.monotonic()-read_start};save();metrics()
  h=metrics();assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in c.records if r['label'].startswith('all-images-'));a['telemetry']={r['label']:{'caller_ms':r['wall_ms'],'metrics':h[r['statement_id']]['metrics']} for r in c.records if r['label'].startswith(('merge-','all-images-'))};a['wall_s']=time.monotonic()-start;a['state']='Range32 second-batch MERGE preserves complete190k edge images';a['qualification']='Private clone of fully qualified39.99M range32 version52; second distinct100k MERGE with explicit physical partition plus full hash/native identity/version predicate. Complete190k20-field images match independent second-batch oracle. Clone/configuration/mutation handles and costs retained; no publication descriptor, source ACK, permanent history extension, global postapply sweep, real concurrent fence, sustained rate, cold/caller or billion admission. Zero CDF read-byte counters do not prove zero I/O. No original mutation, maintenance or compute resize.';save();print(json.dumps({'state':a['state'],'checks':{k:v['cdf_read_caller_s'] for k,v in a['checks'].items()},'costs':a['costs'],'wall_s':a['wall_s']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same handles and private commits; no blind replay or support claim',error=str(e));save();raise
if __name__=='__main__':main()
