"""Read-only live eligibility check; not integrated publisher timing or fencing."""
import hashlib,json,time
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent

def main():
 paths={'vector':'out/native/ashlar_fifth_guard_publish_r481/audited-summary.json','inputs':'out/native/ashlar_sixth_delta_stage_r533/audited-summary.json','budget':'out/sixth-publisher-budget-r535.json','node_history':'out/native/ashlar_storage_history_r255/summary.json'}
 sources={k:json.loads((B/p).read_text()) for k,p in paths.items()}
 assert sources['vector']['state']=='Integrated private fifth100k guarded publication passes full change custody'
 assert sources['inputs']['state']=='Four complete normalized input roles match independent source digests and are Delta-version pinned'
 tables={**{'base-'+k:v for k,v in sources['vector']['tables'].items()},**{'input-'+k:v for k,v in sources['inputs']['tables'].items()}}
 assert len(tables)==10
 O=B/'out/native/ashlar_sixth_publisher_preflight_r536';assert not O.exists();O.mkdir();start=time.monotonic();c=Client(O,observation_timeout=150,cancel_after=120)
 a={'state':'Checking live heads and physical profiles','source_sha256':{k:hashlib.sha256((B/p).read_bytes()).hexdigest() for k,p in paths.items()},'sources':paths,'tables':{},'bounds':{'read_bytes':10000000,'write_remote_bytes':0,'spill_to_disk_bytes':0,'wall_s':180},'qualification':'Read-only eligibility observation. Integrated publisher must repeat UUID/head/profile checks inside its ready-input clock. No predecessor scan, mutation, descriptor advance, fence, ACK or throughput admission.'}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def rows(label,q):
  r=c.sql(label,q);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(cols,x)) for x in r]
 save()
 try:
  for role,t in tables.items():
   d=rows('detail-'+role,'DESCRIBE DETAIL '+t['table'])[0];assert d['id']==t['id'] and d['format']=='delta',role
   h=rows('history-'+role,'DESCRIBE HISTORY '+t['table']+' LIMIT 1')[0];
   if role=='base-object_current':
    assert int(h['version'])==8 and h['queryHistoryStatementId']=='8b6cf102-d6ed-4eaa-93d3-e904ef8cbcc1'
    assert '8b6cf102-d6ed-4eaa-93d3-e904ef8cbcc1' in json.dumps(sources['node_history'])
    assert c.sql('pinned-readonly-node-count','SELECT count(*) FROM '+t['table']+' VERSION AS OF 6')==[['8000000']]
   else:assert int(h['version'])==t['version'],(role,h['version'],t['version'])
   schema=rows('schema-'+role,'DESCRIBE TABLE '+t['table']);a['tables'][role]={'pin':t,'detail':d,'head':h,'schema':schema};save()
   if role in ['base-edge_current','base-adjacency_forward']:
    assert json.loads(d['properties']).get('delta.enableChangeDataFeed')=='true',role
   if role in ['base-source_record','base-property_journal']:assert 'rowTracking' in json.loads(d['tableFeatures'])
   if role=='base-edge_current':assert json.loads(d['partitionColumns'])==[] and json.loads(d['clusteringColumns'])==['lookup_hash']
   if role.startswith('input-'):
    assert t['version']==0 and h['queryHistoryStatementId']==t['statement_id']
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  a['costs']={k:sum(x['metrics'].get(k,0) for x in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
  assert all(v<=a['bounds'][k] for k,v in a['costs'].items());a['wall_s']=time.monotonic()-start;assert a['wall_s']<180
  a['state']='Nine exact mutable/input heads and unchanged pinned node6 eligible for integrated sixth guarded publisher';save();print(json.dumps({'state':a['state'],'costs':a['costs'],'wall_s':a['wall_s']}))
 except Exception as e:a.update(state='Stopped; same handles only; no mutation or publication',error=str(e));save();raise
if __name__=='__main__':main()
