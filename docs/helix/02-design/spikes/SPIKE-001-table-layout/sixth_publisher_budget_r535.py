"""Bound actual sixth stage/parent/oracles before integrated publisher admission."""
import hashlib,json
from pathlib import Path
B=Path(__file__).resolve().parent

def main():
 paths={'vector':'out/native/ashlar_fifth_guard_publish_r481/audited-summary.json','inputs':'out/native/ashlar_sixth_delta_stage_r533/audited-summary.json','admission':'out/sixth-local-admission-r530.json','cdf':'out/sixth-cdf-image-oracle-r529.json','tomb':'out/sixth-canonical-tomb-r529.json','guard':'out/native/ashlar_inline_guard_finish_r425/audited-summary.json','validation':'out/validation-stage-audit-r511.json','transfer':'out/native/ashlar_sixth_batch_upload_r531/audited-summary.json'};s={k:json.loads((B/p).read_text()) for k,p in paths.items()}
 assert s['vector']['state']=='Integrated private fifth100k guarded publication passes full change custody' and s['inputs']['state']=='Four complete normalized input roles match independent source digests and are Delta-version pinned'
 assert s['admission']['publisher_sha256']==hashlib.sha256((B/'sixth_guard_publish_r537.py').read_bytes()).hexdigest()
 assert s['transfer']['download_verified_bytes']==s['admission']['source_bytes']==1100634578
 assert {k:v['rows'] for k,v in s['inputs']['checks'].items() if isinstance(v,dict) and 'rows' in v}=={'source_record':100000,'property_journal':216667,'tombstone':10000,'current_replacement':90000}
 assert all(t['version']==0 for t in s['inputs']['tables'].values())
 bound={'read_bytes':100000000000,'write_remote_bytes':3000000000,'spill_to_disk_bytes':1000000000,'wall_s':600,'statement_cancel_after_s':120}
 result={'format':'ashlar-sixth-concurrent-validation-publisher-budget/1','status':'Admission budget; sixth role mutation not started','sources':paths,'source_sha256':{k:hashlib.sha256((B/p).read_bytes()).hexdigest() for k,p in paths.items()},'base_vector':s['vector']['tables'],'inputs':s['inputs']['tables'],'changes':100000,'updates':90000,'deletes':10000,'journal_appends':216667,'batch_id':'mixed-change/6','source_position_interval':[500000,600000],'expected_final_rows':s['admission']['expected_final_rows'],'stages':{'integrated_publisher':bound},'qualification':'Existing authorized compute, same wide current layout and full20field atomic guards, serial5 role writes, four-worker15-check post-commit validation and complete ten-table closing profile custody. Prior fifth run read39.819GB/wrote0.503GB in233.213s; independent concurrent post-commit stage29.517s. These motivate bounded100GB/3GB/600s ceilings, not predict60s or steady10k/s. All preparation separately charged. No clone-per-batch, cleanup, production descriptor/ACK, new compute or shared resize. Fresh native input/base eligibility required before role writes.'};(B/'out/sixth-publisher-budget-r535.json').write_text(json.dumps(result,indent=2)+'\n');print(result['status'])
if __name__=='__main__':main()
