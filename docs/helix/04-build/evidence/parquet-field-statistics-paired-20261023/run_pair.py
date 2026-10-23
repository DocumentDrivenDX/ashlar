"""Sequential two-case supervisor; prepared only, do not auto-run or retry."""
from __future__ import annotations
import argparse, hashlib, json, os, signal, subprocess, sys, time
from pathlib import Path

PYTHON='/private/tmp/ashlar-release-20261021-m3rde5p2/venv/bin/python'
REPO='/Users/erik/Projects/ashlar'
WHEEL='/private/tmp/ashlar-release-20261021-m3rde5p2/wheelhouse-bc2accd8/ashlar_graph_toolkit-0.1.0.dev1-py3-none-any.whl'
COMMIT='bc2accd883a49d34c282dc002af98b4b05ea9440'
JAVA_HOME='/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home'
EVIDENCE=Path(__file__).resolve().parent
TIME_CAP=300;TERMINATE_AT=290;OUTPUT_CAP=10_000_000_000;DIAG_CAP=128_000_000;POLL=.5;GRACE=8
SELECTED_TYPES={'source_system':str,'lookup_hash':int,'rel_type_id':int,'id':int}

def tree_bytes(path):
 total=0
 if path.exists():
  for p in path.rglob('*'):
   if p.is_file():
    try:total+=p.stat().st_size
    except FileNotFoundError:pass
 return total
def output_bytes(root):return tree_bytes(root)
def parquet_data_bytes(root):
 total=0
 for warehouse in (root/'baseline/out/warehouse',root/'candidate/out/warehouse'):
  if warehouse.exists():
   for path in warehouse.rglob('*.parquet'):
    if path.is_file():
     try:total+=path.stat().st_size
     except FileNotFoundError:pass
 return total
def diagnostic_bytes(root):
 # Count every non-Parquet artifact, including Delta transaction logs.
 return max(0,tree_bytes(root)-parquet_data_bytes(root))
def terminate_group(proc,started):
 try:os.killpg(proc.pid,signal.SIGTERM)
 except ProcessLookupError:pass
 remaining=max(0,299-(time.monotonic()-started))
 try:proc.wait(timeout=min(GRACE,remaining))
 except subprocess.TimeoutExpired:
  try:os.killpg(proc.pid,signal.SIGKILL)
  except ProcessLookupError:pass
  proc.wait()
def stage_for_write(att):
 insert=next((x for x in att.get('sql',[]) if x.get('kind')=='edge-insert'),None)
 if insert is None:return None
 candidates=[]
 for sql in att.get('sql',[]):
  if str(sql.get('rootExecutionId'))!=str(insert.get('executionId')):continue
  for job in sql.get('jobs',[]):
   for sid in job.get('stage_ids',[]):
    stage=att.get('stages',{}).get(str(sid),{});metrics=stage.get('task_metric_totals',{})
    if metrics.get('output_records')==225_000_000:candidates.append((sql,job,sid,stage,metrics))
 if len(candidates)!=1:return {'error':'expected exactly one output stage for 225M records','candidate_count':len(candidates)}
 sql,job,sid,stage,metrics=candidates[0]
 return {'outer_execution_id':insert['executionId'],'child_execution_id':sql.get('executionId'),'child_root_execution_id':sql.get('rootExecutionId'),
  'job_id':job.get('job_id'),'stage_id':sid,'tasks_expected':stage.get('tasks_expected'),'tasks_observed':stage.get('tasks_observed'),
  'submission_time':stage.get('submission_time'),'completion_time':stage.get('completion_time'),
  'stage_span_ms':(stage.get('completion_time')-stage.get('submission_time')) if type(stage.get('completion_time'))is int and type(stage.get('submission_time'))is int else None,
  'task_metric_totals':metrics}
def normalized_stats(case):
 rows=[]
 for item in case['delta_selected_stats']:
  rows.append({k:item[k] for k in ('numRecords','min_keys','max_keys','null_count_keys','selected_min','selected_max','selected_null_count')})
 return sorted(rows,key=lambda x:json.dumps(x,sort_keys=True))
def validate_case_stats(case):
 selected={'lookup_hash','source_system','rel_type_id','id'}
 if len(case.get('delta_selected_stats',[]))!=18:return False
 if sum((r.get('numRecords') if type(r.get('numRecords'))is int else 0) for r in case['delta_selected_stats'])!=225_000_000:return False
 for row in case['delta_selected_stats']:
  if any(row.get(k)!=sorted(selected) for k in ('min_keys','max_keys','null_count_keys')):return False
  if any(type(row.get(k))is not dict or set(row[k])!=selected for k in ('selected_min','selected_max','selected_null_count')):return False
  if any(type(v)is not int or v<0 for v in row['selected_null_count'].values()):return False
  if any(type(row[name][key]) is not SELECTED_TYPES[key] for name in ('selected_min','selected_max') for key in selected):return False
 chunks=case.get('footer_selected_stats_by_chunk')
 if type(chunks)is not list or len(chunks)!=case.get('footer_row_group_count'):return False
 for chunk in chunks:
  columns=chunk.get('columns')
  if type(columns)is not dict or set(columns)!=selected:return False
  for value in columns.values():
   if not value.get('has_non_null_value') or type(value.get('min'))is not str or type(value.get('max'))is not str:return False
   if type(value.get('physical_type'))is not str:return False
   nulls=value.get('num_nulls')
   if nulls is not None and (type(nulls)is not int or nulls<0):return False
 return True
def compare_cases(root):
 baseline=json.loads((root/'baseline/out/case-summary.json').read_text());candidate=json.loads((root/'candidate/out/case-summary.json').read_text())
 mismatches=[]
 if not validate_case_stats(baseline):mismatches.append('baseline-selected-statistics-evidence')
 if not validate_case_stats(candidate):mismatches.append('candidate-selected-statistics-evidence')
 for key in ('insert_sql_sha256','rows','files','codecs'):
  if baseline[key]!=candidate[key]:mismatches.append(key)
 lookup_equal=baseline['lookup']['row']==candidate['lookup']['row']
 if not lookup_equal:mismatches.append('exact-lookup-row')
 bstats=baseline['footer_statistics_summary'];cstats=candidate['footer_statistics_summary']
 keys=sorted(set(bstats)|set(cstats))
 changed={k:{'baseline':bstats.get(k),'candidate':cstats.get(k)} for k in keys if bstats.get(k)!=cstats.get(k)}
 if set(changed)!={'source_delivery_id'}:mismatches.append('per-column-Parquet-statistics-scope')
 if normalized_stats(baseline)!=normalized_stats(candidate):mismatches.append('Delta-selected-stats')
 if baseline.get('footer_selected_stats_by_chunk')!=candidate.get('footer_selected_stats_by_chunk'):mismatches.append('Parquet-selected-stats-by-row-group')
 if baseline['settings']['selected_key_statistics']!=candidate['settings']['selected_key_statistics']:mismatches.append('selected-key-writer-settings')
 bstage=baseline.get('write_stage');cstage=candidate.get('write_stage')
 if not bstage or not cstage or bstage.get('task_metric_totals',{}).get('output_records')!=225_000_000 or cstage.get('task_metric_totals',{}).get('output_records')!=225_000_000:mismatches.append('write-stage-attribution')
 result={'baseline_write_stage':bstage,'candidate_write_stage':cstage,'baseline_insert_seconds':baseline['insert_seconds'],'candidate_insert_seconds':candidate['insert_seconds'],
  'insert_seconds_delta':candidate['insert_seconds']-baseline['insert_seconds'],'baseline_parquet_bytes':baseline['parquet_bytes'],'candidate_parquet_bytes':candidate['parquet_bytes'],
  'parquet_bytes_delta':candidate['parquet_bytes']-baseline['parquet_bytes'],
  'baseline_lookup_source_file_inventory_count':baseline['lookup']['source_file_inventory_count'],'candidate_lookup_source_file_inventory_count':candidate['lookup']['source_file_inventory_count'],
  'baseline_lookup_source_file_inventory_bytes':baseline['lookup']['source_file_inventory_bytes'],'candidate_lookup_source_file_inventory_bytes':candidate['lookup']['source_file_inventory_bytes'],
  'footer_statistics_differences':changed,'delta_selected_stats_equal':normalized_stats(baseline)==normalized_stats(candidate),
  'lookup_equal':lookup_equal,'lookup_executed_plan_baseline':baseline['lookup']['executed_plan'],'lookup_executed_plan_candidate':candidate['lookup']['executed_plan'],
  'lookup_source_file_inventory_is_not_a_scanned_file_measurement':True,'selected_statistics_evidence_valid':validate_case_stats(baseline) and validate_case_stats(candidate),
  'mismatches':mismatches,'candidate_admissible_for_review':not mismatches,
  'performance_interpretation':'One unprofiled sequential pair only; no general speed claim. JFR is intentionally omitted.'}
 (root/'pair-comparison.json').write_text(json.dumps(result,sort_keys=True,indent=2)+'\n')
 return result

def main():
 p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);a=p.parse_args();root=a.root.resolve()
 if root.exists() or not root.is_relative_to(Path('/private/tmp')):raise SystemExit('fresh unique pair root under /private/tmp required')
 root.mkdir(mode=0o700,parents=True)
 env=os.environ.copy();env.update({'JAVA_HOME':JAVA_HOME,'PATH':JAVA_HOME+'/bin:'+os.environ.get('PATH',''),
   'PYSPARK_PYTHON':PYTHON,'PYSPARK_SUBMIT_ARGS':'--driver-memory 24g pyspark-shell'})
 commands=[[PYTHON,'-B',str(EVIDENCE/'run_case.py'),'--repo',REPO,'--wheel',WHEEL,'--commit',COMMIT,'--out',str(root/name/'out'),'--case',name] for name in ('baseline','candidate')]
 if not Path(PYTHON).is_file() or not Path(WHEEL).is_file():raise SystemExit('reviewed existing runtime/wheel not available; no installation attempted')
 started=time.monotonic();results=[];limit=None;returncode=None
 for name,command in zip(('baseline','candidate'),commands):
  elapsed=time.monotonic()-started;out=output_bytes(root);diag=diagnostic_bytes(root)
  if elapsed>=TERMINATE_AT:limit='wall-300s-before-'+name
  elif out>OUTPUT_CAP:limit='output-10GB-before-'+name
  elif diag>DIAG_CAP:limit='diagnostics-128MB-before-'+name
  if limit:break
  log_path=root/(name+'.log')
  with log_path.open('xb') as log:
   proc=subprocess.Popen(command,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
   while proc.poll() is None:
    elapsed=time.monotonic()-started;out=output_bytes(root);diag=diagnostic_bytes(root)
    if elapsed>=TERMINATE_AT:limit='wall-300s'
    elif out>OUTPUT_CAP:limit='output-10GB'
    elif diag>DIAG_CAP:limit='diagnostics-128MB'
    if limit:terminate_group(proc,started);break
    time.sleep(POLL)
   code=proc.wait()
  results.append({'case':name,'returncode':code,'log':log_path.name,'elapsed_seconds':round(time.monotonic()-started,3)})
  returncode=code
  if code!=0 and limit is None:limit='child-failed-'+name
  if limit:break
 output=output_bytes(root);diag=diagnostic_bytes(root);elapsed=time.monotonic()-started
 if limit is None and output>OUTPUT_CAP:limit='output-10GB-observed-after-exit'
 if limit is None and diag>DIAG_CAP:limit='diagnostics-128MB-observed-after-exit'
 comparison=None
 if limit is None and returncode==0 and len(results)==2:
  comparison=compare_cases(root)
 result={'cases':results,'returncode':returncode,'elapsed_seconds':round(elapsed,3),'limit_triggered':limit,'output_bytes':output,'diagnostic_bytes':diag,
  'limits':{'wall_seconds':TIME_CAP,'wall_termination_trigger_seconds':TERMINATE_AT,'output_bytes':OUTPUT_CAP,'diagnostic_bytes':DIAG_CAP,'poll_seconds':POLL,'terminate_grace_seconds':GRACE,'thresholds_are_polled_not_filesystem_quotas':True},
  'sequential_no_retry':True,'jfr_enabled':False,'comparison':comparison,'commands':commands}
 (root/'supervisor-result.json').write_text(json.dumps(result,sort_keys=True,indent=2)+'\n')
 print(json.dumps(result,sort_keys=True))
 return 0 if limit is None and returncode==0 and comparison and comparison['candidate_admissible_for_review'] else 1
if __name__=='__main__':raise SystemExit(main())
