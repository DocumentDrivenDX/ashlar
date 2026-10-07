"""Correct schema metadata with new immutable private descriptor, preserve original."""
import json,time
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_mixed_descriptor_r234';assert not O.exists();s=json.loads((B/'out/native/ashlar_mixed_apply_r232/summary.json').read_text());c=Client(O);manifest=s['manifest'];a={'state':'running','original_processing_s':s['processing_s'],'qualification':'Original r232 descriptor has empty schema map; original timing not transferred to corrected descriptor. New private descriptor pins identical data, correction only; no data mutation, new compute or producer/throughput admission.'}
original=c.sql('original',f"SELECT publication_id,profile_version,table_versions_json,source_progress_json,schema_revisions_json,validation_report_json FROM {manifest} WHERE publication_id='mixed-r232'");assert len(original)==1 and original[0][4]=='{}';assert json.loads(original[0][2])==s['publication_vector']
# Verify native schema revisions for both current roles at published versions.
for role in ['object_current','edge_current']:
 table=s['tables'][role]['table'];v=s['versions'][role];assert c.sql('revision-'+role,f'SELECT DISTINCT source_feed,schema_revision FROM {table} VERSION AS OF {v}')==[['synthetic-scale-mixed','synthetic-mixed/1']]
values=original[0].copy();values[0]='mixed-r234';values[4]=json.dumps({'synthetic-scale-mixed':'synthetic-mixed/1'},sort_keys=True,separators=(',',':'))
def lit(x):return "decode(unhex('"+x.encode().hex()+"'),'UTF-8')"
c.sql('publish-corrected',f'INSERT INTO {manifest} SELECT '+','.join(lit(x) for x in values)+',current_timestamp()');assert c.sql('corrected-readback',f"SELECT publication_id,profile_version,table_versions_json,source_progress_json,schema_revisions_json,validation_report_json FROM {manifest} WHERE publication_id='mixed-r234'")==[values]
a['corrected_elapsed_host_wall_s']=time.time()-s['processing_start_epoch'];a['clock_qualification']='Elapsed host wall clock since original ready-input start; includes original post-publication diagnostics, intervening audit and metadata correction. Assumes no host clock jump; not original monotonic service clock, causal metadata-only overhead or service p95.'
for n in range(12):
 try:h=collect_history(c.w,c.records,O/'shared-history.json');break
 except HistoryPending:
  if n==11:raise
  time.sleep(2)
a['costs']={k:sum(x['metrics'].get(k,0) for x in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};a['state']='Corrected schema-revision descriptor exactly verified';a['values']=values;(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n');print(json.dumps({k:v for k,v in a.items() if k!='values'},indent=2))
