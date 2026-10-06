"""Summarize measured native history without implying billion-node admission."""
from pathlib import Path
import json,math,statistics
BASE=Path(__file__).resolve().parent
summary=[]
def pct(values,q):
 v=sorted(values);return round(v[max(0,math.ceil(q*len(v))-1)],1)
for folder in sorted((BASE/'out/native').iterdir()):
 if not (folder/'measurements.json').exists():continue
 meta=json.loads((folder/'run.json').read_text());measure=json.loads((folder/'measurements.json').read_text());groups=[]
 for table in ('bag_l','bag_z','promoted_l','type_a_l'):
  for shape in ('lookup','list','count'):
   rows=[x for x in measure if x['table']==table and x['shape']==shape];valid=[r for r in rows if r['metrics'] and r['metrics'].get('result_from_cache') is False]
   if not valid:continue
   group={'table':table,'shape':shape,'samples':len(valid),'cached_or_missing_samples':len(rows)-len(valid)}
   for column,key in [('execution','execution_time_ms'),('compile','compilation_time_ms'),('server_total','total_time_ms')]:
    values=[x['metrics'][key] for x in valid if key in x['metrics']]
    if values:group[column+'_p50_ms']=pct(values,.5);group[column+'_p95_ms']=pct(values,.95)
   group['cli_wall_p50_ms']=pct([x['wall_ms'] for x in valid],.5);group['cli_wall_p95_ms']=pct([x['wall_ms'] for x in valid],.95)
   for key in ['read_bytes','read_files_count','pruned_files_count','bytes_read_from_cache_percentage']:
    values=[x['metrics'][key] for x in valid if key in x['metrics']]
    if values:group[key+'_median']=statistics.median(values)
   groups.append(group)
 records=[json.loads(x) for x in (folder/'statements.jsonl').read_text().splitlines()]
 environment=next(r['response']['result']['data_array'] for r in records if r['label']=='environment')
 details={r['label']:r['response'].get('result',{}).get('data_array',[]) for r in records if r['label'].startswith('detail-')}
 summary.append({'meta':meta,'runtime':environment,'groups':groups,'table_details':details,'result_parity':'passed','result_cache_hits':sum(x.get('metrics',{}).get('result_from_cache') is True for x in measure)})
(BASE/'out/native-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
text='''---
ddx:
  id: ashlar.native-layout-evidence
  type: evidence
  activity: design
  status: draft
  authoring:
    home: repo
---

# Native Databricks layout evidence

Executed 2026-10-05 on dbw-aidev-cus, existing data-gateway serverless Photon
SQL warehouse, 2X-Small, one cluster. SQL channel 2026.38. CLI profile aidev-cus
handled authentication; credentials are not captured in evidence.

## Scope and limits

Synthetic fixtures; exact schema/run IDs and native statement IDs are retained.
Result cache is avoided using uuid() in read output and verified against query
history; disk cache remains enabled/observed. Warm running compute only. CLI
wall timing includes process/auth/network/response handling and is not optimized
production-driver latency. Native history separates execution, compilation and
server-total duration. Counts are full scans at the tested small scale, not
proof of interactive full-graph aggregation at 1B nodes.

The larger fixture is 3M nodes / 9M edges, not the 1B-node / 5B-edge planning
graph. Uniform repeated payloads compress unrealistically well. Fixed-layout
runs on shared existing compute do not prove cold IO, concurrency, ingestion
freshness, pruning at production file counts or external graph compatibility.
No full 1B-node support or p99 result is claimed.

'''
for run in summary:
 m=run['meta'];text+=f"## {m['rows_per_type']*3:,} nodes / {m['rows_per_type']*9:,} edges\n\n"
 text+=f"Schema: `{m['catalog']}.{m['schema']}`. {m['repeats']} recorded repetitions per table/query, one excluded warmup. {run['result_cache_hits']} result-cache hits. Result parity and endpoint checks passed.\n\n"
 text+='| Table | Shape | n | Execution p95 ms | Compile p95 ms | Server total p95 ms | CLI wall p95 ms | Read/pruned files median |\n| --- | --- | --- | --- | --- | --- | --- | --- |\n'
 for g in run['groups']:
  text+=f"| {g['table']} | {g['shape']} | {g['samples']} | {g.get('execution_p95_ms','unknown')} | {g.get('compile_p95_ms','unknown')} | {g.get('server_total_p95_ms','unknown')} | {g['cli_wall_p95_ms']} | {g.get('read_files_count_median','unknown')} / {g.get('pruned_files_count_median','unknown')} |\n"
 text+='\n'
text+='''## Larger-run interpretation

At 3M nodes / 9M edges, singleton execution p95 is 256 ms for generic liquid,
276 ms for partitioned/Z-order, 299 ms for shared promoted and 247 ms for typed.
Server-total p95 is 394–458 ms; CLI wall p95 is 845–1054 ms. The provisional
100 ms execution / 250 ms caller gate remains failed. Removing CLI overhead
alone cannot close the server-total gap.

Typed list/count median execution is 192/217 ms versus bag-liquid 568/553 ms
(approximately 3.0×/2.5× faster). This supports scalar serving projections,
not a universal change to canonical per-type tables. Singleton differences do
not establish a statistically robust clustering winner from twenty samples.

Generic liquid and promoted scans read one file and prune zero files in the
median; the type-partitioned variant reads one and prunes two. Thus the highly
compressible fixture still does not exercise realistic many-file point pruning.
Measured read bytes came from IO cache (median 100%). No cold-data result,
concurrency result or billion-node extrapolation is supported.

## Native Contract probes

In the small-run schema all seven CONTRACT-003 CREATE TABLE statements succeeded.
Stored exact JSON strings recovered unchanged, including int64 max, a decimal
beyond common float precision, explicit null, nanosecond timestamp text/offset
and unknown nested retained content. These are carrier preservation tests, not
native typed-value or UMF adapter admission. Version-pinned lookup returned
version 1 after version 2 replaced current state. A duplicate synthetic key was
accepted by Delta and detected by a grouping check, confirming publisher-owned
semantic uniqueness. Full multi-table publication/recovery remains untested.

## Findings and decision gates

The initial proposed 100 ms execution / 250 ms caller singleton gates are not
satisfied by this warehouse/path. Do not raise targets silently or claim that
clustering guarantees point-read latency. The small run has too few files to
choose layout from pruning alone. Interpret larger-run metrics before selecting
identity clustering, typed projections or Z-order. A faster persistent client
can remove CLI overhead, but cannot erase measured warehouse compilation and
execution time.

Keep the native singleton path and generic canonical objects/edges. The current
ADR remains proposed; native execution evidence removes the DDL unknown but does
not settle billion-node physical layout. Next experiments: optimized persistent
SQL client, random identity working set larger than cache, more files/skew/types,
concurrency, and manifest/failure recovery. Advance scale only after tail-latency
and cost gates are reviewed.

## Raw evidence

[Native summary](SPIKE-001-table-layout/out/native-summary.json),
[run directories](SPIKE-001-table-layout/out/native/),
[SQL runner](SPIKE-001-table-layout/native_sql.py),
[Contract probe](SPIKE-001-table-layout/native_contract_probe.py).
Each run retains statements.jsonl, query-history.json, measurements.json and
run.json. The small schema also retains contract-probes.jsonl and its summary.
Tables remain isolated in the named schemas for inspection. No warehouse settings,
existing schemas or existing datasets were changed; no automatic schema/table
cleanup was performed.
'''
(BASE.parent/'native-layout-evidence.md').write_text(text)
print('Summarized',len(summary),'completed native runs')
