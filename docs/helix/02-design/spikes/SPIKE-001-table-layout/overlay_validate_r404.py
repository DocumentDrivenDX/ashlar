"""Read-only completion after authoritative canceled outer join; no write replay."""
import hashlib,json,time
from pathlib import Path
from normalized_apply_sql_r276 import pin
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
from overlay_sql_r395 import FIELDS
B=Path(__file__).resolve().parent

def main():
 prior=B/'out/native/ashlar_overlay_build_r401';old=json.loads((prior/'summary.json').read_text());bad=json.loads((prior/'canceled-native-final.json').read_text());assert old['state'].startswith('Stopped;') and bad['is_final'] and bad['status']=='CANCELED';plan=json.loads((B/old['sources']['plan']).read_text());pub=json.loads((B/plan['sources']['publication']).read_text());oracle=json.loads((B/old['sources']['oracle']).read_text());assert hashlib.sha256((B/plan['sources']['publication']).read_bytes()).hexdigest()==plan['source_sha256']['publication'];assert old['base']==pub['base']['edge_current'] and old['reference']==pub['tables']['edge_current']
 groups=pub['checks']['edge_current']['groups'];post=next(x for x in groups if x[0]=='update_postimage');assert post[1:] == ['90000','6','6',oracle['live']['digest']] and pub['checks']['edge_current']['fields']==list(FIELDS)
 O=B/'out/native/ashlar_overlay_validate_r404';assert not O.exists();O.mkdir();start=time.monotonic();c=Client(O,observation_timeout=150,cancel_after=120);a={'state':'Completing logical-key and deletion validation','source_sha256':{'stopped_summary':hashlib.sha256((prior/'summary.json').read_bytes()).hexdigest(),'canceled_native':hashlib.sha256((prior/'canceled-native-final.json').read_bytes()).hexdigest(),'publication':hashlib.sha256((B/plan['sources']['publication']).read_bytes()).hexdigest()},'table':old['table'],'base':old['base'],'reference':old['reference'],'bounds':{'read_bytes':10000000000,'write_remote_bytes':0,'spill_to_disk_bytes':1000000000,'wall_s':180},'checks':{'all90k_live_equivalence':'Independent complete20field native overlay digest equals previously audited E6 complete90k update_postimage digest, with exact E5→E6 commit custody and full predecessor/uniqueness integrity. SHA256 collision assumption; canceled join is not counted as a passing comparison.'}}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def objs(label,q):
  r=c.sql(label,q);return [dict(zip([x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']],row)) for row in r]
 save()
 try:
  table=old['table'];assert objs('initial-overlay-history','DESCRIBE HISTORY '+table['table']+' LIMIT 10')==old['history'];assert objs('initial-overlay-detail','DESCRIBE DETAIL '+table['table'])[0]['id']==table['id'];overlay=pin(table['table'],1);edge=pin(old['reference']['table'],6);bp=pin(old['base']['table'],5);np=pin(old['node']['table'],old['node']['version']);on=' AND '.join('e.'+f+'=o.'+f for f in ['lookup_hash','source_system','rel_type_id','id'])
  assert c.sql('reference10k-deleted-absent',f'SELECT count(*) FROM {overlay} o INNER JOIN {edge} e ON {on} WHERE o.is_deleted')==[['0']]
  keys=['source_system','rel_type_id','id'];anti=' AND '.join('b.'+f+'=o.'+f for f in ['lookup_hash']+keys)+' AND o.entity_version>=b.entity_version';logical=f"SELECT {','.join('b.'+f for f in keys)} FROM {bp} b LEFT ANTI JOIN {overlay} o ON {anti} UNION ALL SELECT {','.join(keys)} FROM {overlay} WHERE NOT is_deleted";assert c.sql('logical-current-key-count',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)) FROM ({logical})')==[['39970000','39970000']]
  q=f'SELECT count(*),count_if(s.id IS NULL OR t.id IS NULL) FROM {overlay} o LEFT JOIN {np} s ON o.source_system=s.source_system AND o.source_type=s.type_id AND o.source_id=s.id LEFT JOIN {np} t ON o.source_system=t.source_system AND o.target_type=t.type_id AND o.target_id=t.id WHERE NOT o.is_deleted';assert c.sql('new-live-typed-endpoints',q)==[['90000','0']]
  a['checks']['structural']='Unique39.97M effective typed edge keys,10k new deletions absent at E6,90k new typed endpoint closure; complete overlay identity/origin/digests from stopped r401 remain retained';assert objs('final-overlay-history','DESCRIBE HISTORY '+table['table']+' LIMIT 10')==old['history'];a['final_detail']=objs('final-overlay-detail','DESCRIBE DETAIL '+table['table'])[0]
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert all(v<=a['bounds'][k] for k,v in a['costs'].items());a['combined_build_validation_costs']={k:old['costs'][k]+a['costs'][k] for k in a['costs']};assert all(v<=plan['bounds'][k] for k,v in a['combined_build_validation_costs'].items());a['wall_s']=time.monotonic()-start;assert a['wall_s']<180;a['state']='Accepted100k overlay matches qualified E6 change images and effective key integrity';a['qualification']='Complete changed-carrier equality is transitive through independently qualified native SHA multisets, not the canceled132.661s outer join. All stopped/canceled costs remain charged; original600s integrated phase is not reported successful. Read-only follow-up separately timed. No incoming admission, full publication freshness, compaction, caller/cold/concurrency, consumer engine or billion admission.';save();print(json.dumps({k:a[k] for k in ['state','wall_s','costs','combined_build_validation_costs']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same read-only handles',error=str(e));save();raise
if __name__=='__main__':main()
