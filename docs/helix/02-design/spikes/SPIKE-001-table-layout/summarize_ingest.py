"""Summarize measured native serial publication without widening its scope."""
import argparse,json,math
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('schema');a=p.parse_args()
base=Path(__file__).resolve().parent;out=base/'out/native'/a.schema
summary=json.loads((out/'ingest-summary.json').read_text())
history={q['query_id']:q for q in json.loads((out/'query-history.json').read_text())}
records=[json.loads(x) for x in (out/'statements.jsonl').read_text().splitlines()]
def pct(xs,p):return sorted(xs)[math.ceil(len(xs)*p)-1] if xs else None
lines=['# Native incremental ingest screening','','Schema: `'+summary['schema']+'`. Existing serverless Photon 2X-Small warehouse `data-gateway` on dbw-aidev-cus; no compute settings changed. Synthetic data only.','','| Changed entities | Publication wall seconds | Effective entities/s | <=60s single batch |','| --- | --- | --- | --- |']
for b in summary['batches']:
 lines.append(f"| {b['entities']:,} | {b['publication_wall_ms']/1000:.3f} | {b['effective_entities_per_s']:.1f} | {b['publication_wall_ms']<=60000} |")
lines+=['','Publication includes canonical MERGE, property journal append, serving MERGE, table-version discovery, full changed-set parity validation and manifest insert. Stage construction is excluded; CLI/auth/network overhead is included. Property string journal values are validated by decoding each old/new JSON token and comparing against the corresponding staged value. Three different-sized batches are not a p95 distribution. Effective batch rate is not sustained ingest capacity. No edges, deletion/replay handling, source feed adapter, concurrent readers, or failure recovery were exercised.','','Each changed object carries roughly 2KB of deterministic SHA2 hex text. It is more varied than the original repeated-x fixture, but is not a measured production payload distribution. The starting serving table contains 1M objects; this does not prove 1B-node/5B-edge admission.','','| Batch | Post-publication reads | Engine p95 ms | Server total p95 ms | CLI wall p95 ms | Result cache hits |','| --- | --- | --- | --- | --- | --- |']
for batch in range(1,4):
 rs=[r for r in records if r['label'].startswith(f'read-{batch}-')]
 qs=[history[r['statement_id']] for r in rs if r['statement_id'] in history]
 def metric(name):return pct([q.get('metrics',{}).get(name,0) for q in qs],.95)
 lines.append(f"| {batch} | {len(rs)} | {metric('execution_time_ms')} | {metric('total_time_ms')} | {pct([r['wall_ms'] for r in rs],.95):.1f} | {sum(bool(q.get('metrics',{}).get('result_from_cache',False)) for q in qs)} |")
if summary.get('correctness'):lines+=['',summary['correctness']]
lines+=['','Read results carry uuid() to defeat result reuse; inspect raw history for cache and I/O evidence. Ten samples per batch are descriptive and insufficient for p99 or stable population p95. Version-pinned reads ran after publication; they do not establish old-publication availability during writes.','','Raw reproducible evidence: `SPIKE-001-table-layout/out/native/'+a.schema+'/`. Harness: `native_ingest.py`; regenerate this report with `summarize_ingest.py '+a.schema+'`. Freshness, concurrency, cost attribution, graph scale and low-latency gates remain open.']
(base.parent/'native-ingest-evidence.md').write_text('\n'.join(lines)+'\n')
print('\n'.join(lines))
