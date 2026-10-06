# 200k-change publication batching evidence

Observed 2026-10-05 on dbw-aidev-cus SQL channel 2026.38, existing shared
`data-gateway` warehouse `2439e1f2e37ac563`; compute settings unchanged.
Schema: `client_dev.ashlar_stream_20261005_e8`.

Six 200k-object batches become ready every 20 seconds, preserving the modeled
10k changes/s rate. The full 20-second accumulation window is included in
oldest-change freshness. Three disjoint 200k-ID ranges repeat twice: 1.2M
changes across 600k objects. Second-cycle producer old values are chained from
the preceding cycle's new values. Before every MERGE, an atomic check verifies
that exact canonical properties, retained text and version match the staged
old state; this prevents manufacturing a plausible journal from stale values.

The candidate uses overlapped staging and canonical-only atomic publication
with property journal and receipt. Scalar graph columns derive from the pinned
canonical table version; no physical serving copy exists. Producer generation
is prebuilt. Staging, prior-state check, transaction cardinality/value/version/
journal checks, version discovery and manifest publication remain timed.
The bounded id-clustered fixture columns and 128MiB target remain unchanged;
this is not full CONTRACT-003 schema integration or the 16MiB lookup candidate.

| Batch | Processing seconds | Queue seconds | Oldest freshness seconds |
| --- | ---: | ---: | ---: |
| 1 | 23.90 | 0.00 | 43.91 |
| 2 | 20.31 | 3.91 | 44.22 |
| 3 | 20.09 | 4.22 | 44.30 |
| 4 | 20.58 | 4.31 | 44.89 |
| 5 | 18.86 | 4.89 | 43.75 |
| 6 | 20.51 | 3.75 | 44.26 |

All six prior-state, transaction and publication checks passed. Every immutable
snapshot subsequently hydrated 200k changed rows with zero exact property,
retained-content, scalar or version mismatches. Six complete singleton reads
and scalar projections also matched their source expectations. Old snapshots
were checked after repeated identities had received later versions. All 69
statements succeeded; complete metrics were refreshed by existing query IDs.

All six oldest freshness bounds are below 60 seconds, with a maximum 44.89s.
The queue remains approximately four seconds after startup instead of growing
by about five seconds per batch in the 100k control. Median atomic wall time
is 15.71s, staging 6.09s (overlapped), each version discovery 0.49s, and manifest
publication 0.86s. Later processing times cluster around the 20-second arrival
interval; the candidate has little demonstrated headroom. It is promising
two-minute evidence, **not long-run p95 or 100k/s burst admission**.

The batching intervention changes accumulation and amortizes coordination
cost without lowering the source rate or removing preservation checks.
Repeated identities test journal continuity but do not prove replay/deletion,
fencing, source-transaction fidelity, failure recovery or concurrent readers.
Node-only fixture evidence also cannot admit edge load or billion-node scale.
Next integrate full canonical and journal contract columns, test longer arrivals
and representative edges, and measure reads during publication before accepting
physical settings. Native scalar query and external-reader compatibility remain
independent requirements.

A repeated bounded billing read still returned no aggregate rows for this
warehouse/workspace during UTC 20:00–24:00. Missing/delayed usage is not zero
cost; shared-warehouse attribution and the larger-stage spend ceiling remain
unresolved. The probe is retained in `out/native/cost-probe-20261005-refresh/`.

Harness: `SPIKE-001-table-layout/native_stream_batch200k.py`. Evidence:
`SPIKE-001-table-layout/out/native/ashlar_stream_20261005_e8/`, including
schedule, completed summary, publisher/staging records, combined statements
and refreshed query history.
