"""Larger complete native bootstrap accounting, no scale admission."""
import json,hashlib
from pathlib import Path
B=Path(__file__).resolve().parent;S='out/native/ashlar_mixed_materialize_r226/summary.json'
def calculate():
 d=json.loads((B/S).read_text());assert d['state']=='All five native materialized roles pass independent full-row digests and typed identity/endpoint checks'
 counts={'object_current':8000000,'edge_current':40000000,'source_record':48000000,'property_journal':192000000,'adjacency_forward':40000000};roles={}
 for name,x in d['tables'].items():
  roles[name]={'sample_rows':x['rows'],'sample_bytes':x['bytes'],'sample_files':x['files'],'measured_bytes_per_row':x['bytes']/x['rows'],'same_width_8m_40m_bytes':x['bytes']/x['rows']*counts[name]}
 total=sum(x['same_width_8m_40m_bytes'] for x in roles.values())
 prior=json.loads((B/'out/native-mixed-storage-r221.json').read_text())
 return {'format':'ashlar-complete-native-bootstrap-accounting/1','source':S,'source_sha256':hashlib.sha256((B/S).read_bytes()).hexdigest(),'roles':roles,'actual_active_bootstrap_bytes':d['active_role_bytes'],'same_width_8m_40m_total_bytes':total,'prior_tiny_chunk_same_width_bytes':prior['8m_40m_same_width_total_bytes'],'140gb_headroom_before_exclusions':140000000000-total,'width_sensitivity':[{'factor':factor,'bytes':total*factor} for factor in [1,1.5,2]],'exclusions':['Changes/deletions: complete before/after history, extra raw deliveries and tombstones.','Retained versions, DVs, logs/checkpoints, staging and failed uncommitted work.','Publication intent/manifest/control rows and mutation fencing; integrity and arrival scheduling workload.','Marginal prices, resource saturation and full-range larger-table validation costs.'],'admission':'Proposed8M/40M only; all-row native sample is complete bootstrap evidence, not larger graph, steady ingest, cold/warm service or source fencing. Do not omit any role to fit140GB.','next':'Implement exact property change/deletion roles and bounded publication schedule for this complete bootstrap; then assess real write/validation overlap and revise larger-stage reserves.'}
if __name__=='__main__':
 r=calculate();(B/'out/native-complete-storage-r227.json').write_text(json.dumps(r,indent=2)+'\n');print(json.dumps(r,indent=2))
