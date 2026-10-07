"""Audit r101 publication, append overlap and exact pinned singleton cohorts."""
import json
import math
from pathlib import Path
from persistent_sql import Client

B=Path(__file__).resolve().parent
O=B/'out/native/ashlar_parallel_publication_20261007_r101'
s=json.loads((O/'summary.json').read_text())
assert s['state']=='completed parallel publication and bounded singleton contention; correctness checks passed'
all_records=[];histories={}
fresh=json.loads((O/'new-publication-reads/summary.json').read_text())
assert fresh['state']=='passed new published full-carrier reads' and fresh['rows']==30
for folder in (O,O/'journal-lane',O/'reader',O/'new-publication-reads'):
    records=[json.loads(line) for line in (folder/'statements.jsonl').read_text().splitlines()]
    c=Client(folder);c.records=records
    h={q['query_id']:q for q in c.history()}
    assert all(h.get(r['statement_id'],{}).get('is_final') for r in records), 'Refresh history only'
    assert all(r['response']['status']['state']=='SUCCEEDED' for r in records)
    assert not any(h[r['statement_id']]['metrics'].get('result_from_cache') for r in records)
    all_records.extend(records);histories.update(h)
assert len({r['statement_id'] for r in all_records})==len(all_records)
def record(label):
    matches=[r for r in all_records if r['label']==label]
    assert len(matches)==1
    return matches[0]
batch=s['batches'][0]
assert len(s['batches'])==1
for label in ('prepare','capture','journal','apply','publish'): record(label+'-r101-b1')
raw,journal=record('capture-r101-b1'),record('journal-r101-b1')
def interval(r): return r['start_epoch'],r['start_epoch']+r['wall_ms']/1000
a,b=interval(raw),interval(journal)
overlap=max(0,min(a[1],b[1])-max(a[0],b[0]))
assert overlap>0, 'No actual overlapping appends'
merge=next(v for v in batch['commits'] if v['operation']=='MERGE')
m=json.loads(merge['operationMetrics'])
assert int(m['numTargetRowsUpdated'])==100000
assert all(int(m[k])==0 for k in ('numTargetRowsCopied','numTargetRowsInserted','numTargetRowsDeleted'))
batch['merge_metrics']=m
start=s['clock_epoch']+batch['publisher_start_offset_s']
end=s['clock_epoch']+batch['verified_manifest_offset_s']
cohorts={k:[] for k in ('idle','load-before-publisher','load-during-publisher','load-boundary','post','fresh-post')}
for r in all_records:
    label=r['label']
    if label.startswith('fresh-post-'): cohorts['fresh-post'].append(r)
    elif label.startswith(('idle-','post-')): cohorts[label.split('-')[0]].append(r)
    elif label.startswith('load-'):
        lo,hi=interval(r)
        key='load-before-publisher' if hi<=start else 'load-during-publisher' if lo>=start and hi<=end else 'load-boundary'
        cohorts[key].append(r)
cohorts['load-during-publisher-no-remote']=[r for r in cohorts['load-during-publisher'] if histories[r['statement_id']]['metrics'].get('read_remote_bytes',0)==0]
cohorts['load-within-append-overlap']=[r for r in cohorts['load-during-publisher'] if interval(r)[0]>=max(a[0],b[0]) and interval(r)[1]<=min(a[1],b[1])]
def p95(v): return sorted(v)[math.ceil(.95*len(v))-1] if v else None
summaries={name:{'n':len(rows),'caller_p95_ms':p95([r['wall_ms'] for r in rows]),
    'engine_p95_ms':p95([histories[r['statement_id']]['metrics']['execution_time_ms'] for r in rows]),
    'file_reads_p95':p95([histories[r['statement_id']]['metrics'].get('read_files_count',0) for r in rows]),
    'remote_read_queries':sum(histories[r['statement_id']]['metrics'].get('read_remote_bytes',0)>0 for r in rows)} for name,rows in cohorts.items()}
s.update(state='audited parallel publication and exact pinned reads; all final queries uncached',
    append_caller_overlap_s=overlap,reader_cohorts=summaries,
    scope_qualification='One 100k hot-set batch, one reader over 30 fixed large-token keys, pinned old published version12 for idle/load/post and separately new manifest version for fresh-post. No-remote and append-overlap groups are subsets, not disjoint cohorts. Small cohort p95s are observed samples, not graph-wide/steady/cold SLAs. Complete pre-staged input; no sustained or burst-rate admission.',
    phases=[{'label':r['label'],'caller_ms':r['wall_ms'],'start_epoch':r['start_epoch'],'metrics':histories[r['statement_id']]['metrics']} for r in all_records if not r['label'].startswith(('idle-','load-','post-'))])
warehouse=c.w.api_client.do('GET','/api/2.0/sql/warehouses/2439e1f2e37ac563')
s['warehouse_observed_after_run']={k:warehouse.get(k) for k in ('id','name','cluster_size','warehouse_type','enable_serverless_compute','min_num_clusters','max_num_clusters','auto_stop_mins','state')}
s['cost_scope']='Existing shared warehouse; no resize or provisioning operation performed. Configuration observed after run; attributable DBU/dollars unqualified.'
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n')
print(json.dumps({'processing_s':batch['processing_s'],'input_ready_to_manifest_s':batch['complete_input_to_manifest_s'],
    'append_pair_wall_s':batch['append_pair_wall_s'],'append_caller_overlap_s':overlap,'reader_cohorts':summaries},indent=2))
