"""Offline decomposition of exact r103 query IDs; no queries re-executed."""
import json, math
from pathlib import Path
B=Path(__file__).resolve().parent
O=B/'out/native/ashlar_maintained_contention_20261007_r103'
s=json.loads((O/'audited-summary.json').read_text())
batch=s['batches'][0]
start=s['clock_epoch']+batch['publisher_start_offset_s']
end=s['clock_epoch']+batch['verified_manifest_offset_s']
rows=[];history={}
for folder in (O,O/'journal-lane',O/'reader',O/'new-publication-reads'):
    history.update({q['query_id']:q for q in json.loads((folder/'query-history.json').read_text())})
    rows.extend(json.loads(line) for line in (folder/'statements.jsonl').read_text().splitlines())
groups={k:[] for k in ('idle','during','during_no_remote','post','fresh_post')}
for r in rows:
    label=r['label'];key=None
    if label.startswith('idle-'):key='idle'
    elif label.startswith('post-'):key='post'
    elif label.startswith('fresh-post-'):key='fresh_post'
    elif label.startswith('load-') and r['start_epoch']>=start and r['start_epoch']+r['wall_ms']/1000<=end:key='during'
    if key is None:continue
    q=history[r['statement_id']];m=q['metrics']
    assert q['is_final'] and not m.get('result_from_cache')
    v={'query_id':r['statement_id'],'caller_ms':r['wall_ms'],
       'total_server_ms':m['total_time_ms'],'compile_ms':m['compilation_time_ms'],
       'execute_ms':m['execution_time_ms'],'result_fetch_ms':m['result_fetch_time_ms'],
       'caller_minus_server_ms':r['wall_ms']-m['total_time_ms'],
       'queue_to_end_ms':m['queue_end_time_ms']-q['query_start_time_ms'],
       'read_remote_bytes':m.get('read_remote_bytes',0)}
    groups[key].append(v)
    if key=='during' and v['read_remote_bytes']==0:groups['during_no_remote'].append(v)
def p95(v):return sorted(v)[math.ceil(.95*len(v))-1]
summary={key:{'n':len(v),'p95':{k:p95([r[k] for r in v]) for k in v[0] if k!='query_id'}} for key,v in groups.items()}
assert summary['idle']['n']==30 and summary['during']['n']==130 and summary['during_no_remote']['n']==114
out={'source':'r103 immutable saved final histories and exact statement IDs',
     'components':summary,'queries':groups,
     'qualification':'Component p95s refer to different queries and must not be added. queue_to_end includes pre-queue initialization; caller-minus-server includes connector/network/client scheduling, not pure network latency. Closed-loop singleton reader and one finite synthetic publisher only.'}
(B/'out/contention-components-r103.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps(summary,indent=2))
