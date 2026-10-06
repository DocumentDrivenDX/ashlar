"""Audit exact statement IDs, final engine metrics and result-cache state."""
import json,math,statistics,sys
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_scale_20261006_r85'
def records(p):return [json.loads(x) for x in (p/'statements.jsonl').read_text().splitlines()]
refresh_paths=[O]+[O/f'uncached-driver-{i}' for i in range(4)]+[O/'maintenance']
if (O/'uncached-v2-driver-summary.json').exists():refresh_paths += [O/f'uncached-v2-driver-{i}' for i in range(4)]
if '--refresh' in sys.argv:
 for p in refresh_paths:
  c=Client(p);c.records=records(p);c.history()
def items(p):
 q={x['query_id']:x for x in json.loads((p/'query-history.json').read_text())}
 return [(r,q.get(r['statement_id'])) for r in records(p)]
def p95(a):return sorted(a)[math.ceil(.95*len(a))-1]
def screen(a):
 assert len(a)==50,len(a)
 assert all(q and q.get('is_final') for _,q in a),'Final query metrics pending; refresh same history'
 assert all(q['metrics'].get('result_from_cache') is False for _,q in a),'Result-cache contamination'
 r={'reads':len(a),'result_cache_hits':0,'caller_p95_ms':p95([r['wall_ms'] for r,q in a])}
 for k in ['execution_time_ms','compilation_time_ms','read_files_count','read_bytes','read_remote_bytes']:
  v=[q['metrics'][k] for _,q in a];r[k]={'p95':p95(v),'median':statistics.median(v),'min':min(v),'max':max(v)}
 r['provisional_warm_engine_pass']=r['execution_time_ms']['p95']<=100
 r['provisional_warm_caller_pass']=r['caller_p95_ms']<=250
 return r
allreads=sum([items(O/f'uncached-driver-{i}') for i in range(4)],[])
serial=screen([x for x in allreads if x[0]['label'].startswith('serial-pinned-')]);concurrent=screen([x for x in allreads if x[0]['label'].startswith('concurrent-pinned-')]);maintained=screen([x for x in items(O/'maintenance') if x[0]['label'].startswith('maintained-pinned-')])
raw=records(O)
def row(label,rs=raw):
 r=next(x for x in rs if x['label']==label);return r['response']['result']['data_array'][0]
def detail(label,rs=raw):
 r=next(x for x in rs if x['label']==label);cols=r['response']['manifest']['schema']['columns'];data=r['response']['result']['data_array'][0];return dict(zip([x['name'] for x in cols],data))
metrics=json.loads(detail('merge-history')['operationMetrics'])
post=records(O/'post-run');maintenance=records(O/'maintenance')
result={'state':'completed qualified comparison','workload':{'nodes':4000000,'edges':20000000,'changed_edges':200000,'shapes':8,'source_endpoint_nodes':400000,'target_endpoint_nodes':400000,'distinct_endpoint_pairs':400000,'parallel_pairs':'Generic Ashlar fixture, not legal native Truss unique-pair/shared-allocation input','entropy':'Variable SHA-derived strings use repeated digest blocks; compressed current widths near 100 bytes, not independent high-entropy consumer data'},'local':json.loads((B/'out/local-scale-20261006/summary.json').read_text()),'native':{'runtime':json.loads(row('runtime')[0]),'compute':'Existing 2X-Small single-cluster data-gateway warehouse; no resizing/new compute','base_run_elapsed_seconds':json.loads((O/'summary.json').read_text())['elapsed_seconds'],'objects':detail('detail-object'),'edges_before_update':detail('detail-edge'),'edges_after_update':detail('post-update-detail',post),'merge_operation_metrics':metrics,'serial_uncached_pinned':serial,'four_client_uncached_pinned':concurrent,'maintenance':{'caller_ms':next(r['wall_ms'] for r in maintenance if r['label']=='full-cluster-maintenance'),'old_version':1,'new_version':int(row('maintained-version',maintenance)[0]),'optimize_metrics':[json.loads(x[1]) for x in next(r for r in maintenance if r['label']=='full-cluster-maintenance')['response']['result']['data_array']],'detail':detail('maintained-detail',maintenance),'full_row_mismatch_count':int(row('full-row-maintenance-parity',maintenance)[0]),'serial_uncached_pinned':maintained}},'limits':['Initial generation is not sustained ingest or publication freshness','No raw source, property journal, tombstone, adjacency/degree or publication cost in this phase','All measured driver reads have warm data; no cold-data gate claim','REST concurrent check hit result cache and is excluded from file-read performance','No billion-node/5B-edge admission or external-reader qualification','Billing dollars unavailable; active file sizes exclude retained versions, Delta logs and DV sidecars'], 'initial_generator_commit':'f99fce6','initial_update_profile_qualification':'Previous non-ID wrapper is not conformant property-map input; pre-correction timings qualify carrier/file behavior only. Corrected map evidence is separate.','scope':'Complete full-row current-carrier parity and native typed endpoint closure are qualified for this generated dataset. Performance gates remain observed comparisons under the fixed UC Delta architecture.'}
correction=B/'out/property-map-correction-20261006/summary.json'
if correction.exists():result['property_map_correction']=json.loads(correction.read_text())
if (O/'uncached-v2-driver-summary.json').exists():
 corrected=sum([items(O/f'uncached-v2-driver-{i}') for i in range(4)],[])
 result['corrected_profile_reads']={'edge_version':2,'detail':detail('corrected-detail',records(B/'out/property-map-correction-20261006/native')),'serial':screen([x for x in corrected if x[0]['label'].startswith('serial-pinned-')]),'four_clients':screen([x for x in corrected if x[0]['label'].startswith('concurrent-pinned-')])}
(O/'comparison.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'serial':serial,'concurrent':concurrent,'maintained':maintained,'maintenance_ms':result['native']['maintenance']['caller_ms']},indent=2))
