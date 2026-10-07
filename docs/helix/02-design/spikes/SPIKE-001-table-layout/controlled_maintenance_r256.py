"""Owned staging-only maintenance override; no row/compute/retention mutation."""
import json,time
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_controlled_maintenance_r256';assert not O.exists();O.mkdir();prior=json.loads((B/'out/native/ashlar_scale_edges_r252/audited-summary.json').read_text());a={'state':'Disabling inherited maintenance on owned staging only','roles':{},'restore_sql':[]};c=Client(O,observation_timeout=200,cancel_after=180)
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
save()
try:
 for role,t in prior['tables'].items():
  rows=c.sql('detail-'+role,'DESCRIBE DETAIL '+t['table']);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(cols,rows[0]))['id']==t['id']
  before=c.sql('before-'+role,'DESCRIBE TABLE EXTENDED '+t['table']);settings=[r[1] for r in before if r[0]=='Predictive Optimization'];assert settings==['ENABLE (inherited from METASTORE metastore_centralus)']
  c.sql('disable-'+role,'ALTER TABLE '+t['table']+' DISABLE PREDICTIVE OPTIMIZATION');after=c.sql('after-'+role,'DESCRIBE TABLE EXTENDED '+t['table']);settings=[r[1] for r in after if r[0]=='Predictive Optimization'];assert settings==['DISABLE']
  a['roles'][role]={'table':t['table'],'uuid':t['id'],'prior':'ENABLE (inherited from METASTORE metastore_centralus)','confirmed':settings[0],'pinned_data_version':t['version']};a['restore_sql'].append('ALTER TABLE '+t['table']+' INHERIT PREDICTIVE OPTIMIZATION');save()
 for n in range(12):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==11:raise
   time.sleep(2)
 a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert a['costs']['read_bytes']<=100000000 and a['costs']['write_remote_bytes']==0;a['state']='All five owned staging tables confirm DISABLE; restore inheritance SQL retained';a['qualification']='Experimental maintenance profile only, same UUIDs/pinned data versions. Metadata status does not prove queued jobs canceled; recheck heads before next admission. No OPTIMIZE/VACUUM/expiry, data-row or compute configuration change. Production maintenance choice remains unqualified.';save();print(json.dumps({'state':a['state'],'costs':a['costs']},indent=2))
except Exception as e:a.update(state='Stopped; inspect same handles and per-role completed settings before further changes',error=str(e));save();raise
