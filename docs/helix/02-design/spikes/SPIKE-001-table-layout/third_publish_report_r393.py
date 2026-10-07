"""Offline measured disposition and second canonical tombstone oracle provenance."""
import hashlib,json
from pathlib import Path
from mixed_change_queries_r230 import row_hash
B=Path(__file__).resolve().parent

def main():
 p=B/'out/native/ashlar_third_lc_publish_r391';a=json.loads((p/'audited-summary.json').read_text());rs=[json.loads(x) for x in (p/'statements.jsonl').read_text().splitlines()];qs={q['query_id']:q for q in json.loads((p/'shared-history.json').read_text())['queries']}
 receipt=B/'out/ashlar_third_batch_files_r379/audited-summary.json';f=json.loads(receipt.read_text());t=f['roles']['tombstone'];want=json.loads((B/'out/third-canonical-tomb-r391.json').read_text());hashes=[];original=[];known=[]
 for part in t['parts']:
  fp=Path(f['local_root'])/part['name'];data=fp.read_bytes();assert len(data)==part['bytes'] and hashlib.sha256(data).hexdigest()==part['file_sha256']
  lines=data.decode().splitlines();assert len(lines)==part['rows']
  for line in lines:
   row=json.loads(line);hashes.append(row_hash(row,want['fields']));known.append(row_hash(row,t['fields']));original.append(hashlib.sha256(line.encode()).hexdigest())
 digest=lambda hs:hashlib.sha256(''.join(sorted(hs)).encode()).hexdigest()
 assert len(hashes)==10000 and digest(hashes)==want['digest'] and digest(known)==t['all_known_field_digest'] and digest(original)==t['original_input_multiset_digest']
 evidence={'state':'Canonical tombstone oracle independently matches exact transferred input files','rows':10000,'fields':want['fields'],'digest':want['digest'],'source_sha256':{str(x.relative_to(B)):hashlib.sha256(x.read_bytes()).hexdigest() for x in [receipt,B/'out/third-canonical-tomb-r391.json',B/'third_publish_report_r393.py',B/'mixed_change_queries_r230.py']}}
 (B/'out/third-canonical-tomb-audit-r393.json').write_text(json.dumps(evidence,indent=2)+'\n')
 measured={}
 for r in rs:
  if r['label'] in ['all-predecessor-fields','mutation-partition','append-source_record','append-property_journal','append-tombstone','merge-current','merge-forward','unique-edges','deleted-absent','typed-endpoints']:
   m=qs[r['statement_id']]['metrics'];measured[r['label']]={'caller_s':r['wall_ms']/1000,**{k:m.get(k,0) for k in ['execution_time_ms','read_bytes','read_remote_bytes','write_remote_bytes','spill_to_disk_bytes']}}
 report={'state':'Integrated third batch correct; measured ready-input processing exceeds60s','processing_s':a['processing_s'],'preflight_s':a['preflight_s'],'mutation_s':a['mutation_s'],'costs':a['costs'],'statements':a['audit']['native_final_statements'],'cached_labels':a['audit']['cached_labels'],'selected_roles':a['tables'],'statement_breakdown':measured,'observed_head_details':a['active_details'],'inventory_caveat':'DESCRIBE DETAIL reports current physical heads; node head8 detail is not pinned-node6 file inventory. No retained/log/staging/CDF/orphan storage total or money estimate.','gate_disposition':{'ready_input_60s':'failed,257.486s; single batch, not p95','steady_10k_per_s':'not admitted','burst_100k_per_s':'not tested here','warm_caller_250ms':'prior failure remains; no read cohort here','warm_engine_100ms':'not tested here','cold_caller_1s':'not tested here','scale_1B_5B':'not admitted'},'next_intervention':'Investigate exact before-carrier guard inside the existing current MERGE and complete CDF preimages to avoid a duplicate full predecessor scan. Preserve deletion/update transaction rollback and missing/mismatching-row refusal; qualify guarded controls before another full batch. Telemetry and semantic checks remain charged, not removed to improve reported time.','source_sha256':{'publisher_audit':hashlib.sha256((p/'audited-summary.json').read_bytes()).hexdigest(),'report_script':hashlib.sha256((B/'third_publish_report_r393.py').read_bytes()).hexdigest()}}
 (B/'out/third-integrated-publication-report-r393.json').write_text(json.dumps(report,indent=2)+'\n');print(report['state'],report['processing_s'],report['costs'])
if __name__=='__main__':main()
