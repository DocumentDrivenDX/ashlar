import json,math
from pathlib import Path
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_bucket_post_update_reads_r181'
r=[json.loads(x) for x in (O/'statements.jsonl').read_text().splitlines()];h={x['query_id']:x for x in json.loads((O/'query-history.json').read_text())}
assert all(x['response']['status']['state']=='SUCCEEDED' and h[x['statement_id']]['is_final'] and h[x['statement_id']]['status']=='FINISHED' for x in r)
expected=next(x for x in r if x['label']=='oracle')['response']['result']['data_array']
assert len(expected)==30
p95=lambda a:sorted(a)[math.ceil(.95*len(a))-1]
reads={}
for name in ['lc','part']:
 rs=[x for x in r if x['label'].startswith(name+'-read-')];assert len(rs)==30
 for x in rs:
  assert x['response']['result']['data_array']==[expected[int(x['label'].split('-')[-1])]]
  assert not h[x['statement_id']]['metrics'].get('result_from_cache')
 reads[name]={'caller_p95_ms':p95([x['wall_ms'] for x in rs]),**{k+'_p95':p95([h[x['statement_id']]['metrics'].get(k,0) for x in rs]) for k in ['execution_time_ms','read_files_count','read_bytes']},'remote_queries':sum(h[x['statement_id']]['metrics'].get('read_remote_bytes',0)>0 for x in rs)}
costs={k:sum(h[x['statement_id']]['metrics'].get(k,0) for x in r) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
s={'state':'60 exact post-update reads passed; post-query read budget failed','reads':reads,'costs':costs,'budget_bytes':15000000000,'qualification':'30 updated SHA-ranked identities, pinned LC1/bucket9, result cache false; operational comparison with differing histories, not causal isolation or controlled cold. Original worker failed before summary on its post-query cost check; results reconstructed without rerun. No latency/rate/billion admission.'}
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps(s,indent=2))
