"""Bound actual sixth stage/parent/oracles before integrated publisher admission."""
import hashlib,json
from pathlib import Path
B=Path(__file__).resolve().parent

def main():
 paths={'vector':'out/native/ashlar_sixth_guard_publish_r537/audited-summary.json','inputs':'out/native/ashlar_seventh_delta_stage_r593/audited-summary.json','admission':'out/seventh-local-admission-r591.json','cdf':'out/seventh-cdf-image-oracle-r588.json','tomb':'out/seventh-canonical-tomb-r588.json','guard':'out/native/ashlar_inline_guard_finish_r425/audited-summary.json','validation':'out/validation-stage-audit-r511.json','transfer':'out/native/ashlar_seventh_batch_upload_r592/audited-summary.json'};s={k:json.loads((B/p).read_text()) for k,p in paths.items()}
 assert s['vector']['state']=='Integrated private sixth100k guarded publication passes full change custody' and s['inputs']['state']=='Four complete normalized input roles match independent source digests and are Delta-version pinned'
 assert s['admission']['publisher_sha256']==hashlib.sha256((B/'seventh_guard_publish_r596.py').read_bytes()).hexdigest()
 assert s['transfer']['download_verified_bytes']==s['admission']['source_bytes']==1100633880
 assert {k:v['rows'] for k,v in s['inputs']['checks'].items() if isinstance(v,dict) and 'rows' in v}=={'source_record':100000,'property_journal':216667,'tombstone':10000,'current_replacement':90000}
 assert all(t['version']==0 for t in s['inputs']['tables'].values())
 bound={'read_bytes':100000000000,'write_remote_bytes':3000000000,'spill_to_disk_bytes':1000000000,'wall_s':600,'statement_cancel_after_s':120}
 result={'format':'ashlar-seventh-concurrent-closing-publisher-budget/1','status':'Admission budget; seventh role mutation not started','sources':paths,'source_sha256':{k:hashlib.sha256((B/p).read_bytes()).hexdigest() for k,p in paths.items()},'base_vector':s['vector']['tables'],'inputs':s['inputs']['tables'],'changes':100000,'updates':90000,'deletes':10000,'journal_appends':216667,'batch_id':'mixed-change/7','source_position_interval':[600000,700000],'expected_final_rows':s['admission']['expected_final_rows'],'stages':{'integrated_publisher':bound},'qualification':'Existing authorized compute, same wide current layout and full20field atomic guards, serial5 role writes, four-worker15-check post-commit validation and complete concurrent ten-table schema/profile/commit-ID closing custody. Prior sixth run read40.462GB/wrote0.503GB in221.826s; complete concurrent closing module observations4.52/3.02s. These motivate bounded100GB/3GB/600s ceilings, not predict60s or steady10k/s. All preparation separately charged. No clone-per-batch, cleanup, production descriptor/ACK, new compute or shared resize. Fresh native input/base eligibility required before role writes.'};(B/'out/seventh-publisher-budget-r594.json').write_text(json.dumps(result,indent=2)+'\n');print(result['status'])
if __name__=='__main__':main()
