"""Local audit of native scheduling windows; does not infer causal resource effects."""
import json
from pathlib import Path
B=Path(__file__).resolve().parent;out=B/'out/native/ashlar_queue_serial_r197';summary={}
for name,run in [('overlap','ashlar_queue_resume_r192'),('serial','ashlar_queue_serial_r197')]:
 O=B/'out/native'/run;s=json.loads((O/'audited-summary.json').read_text());windows={}
 for lane,label in [('', 'capture-r189-b1'),('journal-lane','journal-r189-b1'),('apply-lane','apply-r189-b1')]:
  r=next(x for x in map(json.loads,(O/lane/'statements.jsonl').read_text().splitlines()) if x['label']==label);h=next(x for x in json.loads((O/lane/'query-history.json').read_text()) if x['query_id']==r['statement_id']);assert h['is_final'] and h['status']=='FINISHED';windows[label]={'query_id':r['statement_id'],'native_start_ms':h['query_start_time_ms'],'native_execution_end_ms':h['execution_end_time_ms'],'caller_ms':r['wall_ms']}
 current=windows['apply-r189-b1'];overlaps={}
 for label in ['capture-r189-b1','journal-r189-b1']:
  w=windows[label];overlaps[label]=max(0,min(w['native_execution_end_ms'],current['native_execution_end_ms'])-max(w['native_start_ms'],current['native_start_ms']))
 if name=='serial':assert all(v==0 for v in overlaps.values()) and max(windows[k]['native_execution_end_ms'] for k in overlaps)<=current['native_start_ms']
 else:assert all(v>0 for v in overlaps.values())
 row=s['batches'][0];summary[name]={'windows':windows,'current_native_statement_overlap_ms':overlaps,'processing_s':row['processing_s'],'uniform_modeled_record_age_p95_s':row['uniform_record_age_p95_s'],'write_lane_wall_s':row['append_pair_wall_s'],'validation_wall_s':row['validation_three_lane_wall_s']}
summary['qualification']='Same pinned input, separate fresh owned clones, sequential observations on shared warehouse. Native query-start/execution-end windows establish statement ordering, not task scheduling/resource causation. Phase telemetry is included; no service-time p95, sustained/burst or billion admission.'
(out/'schedule-comparison.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary,indent=2))
