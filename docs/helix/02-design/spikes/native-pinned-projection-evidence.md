# Pinned scalar projection publication evidence

Observed 2026-10-05 on dbw-aidev-cus SQL channel 2026.38, existing shared
`data-gateway` warehouse `2439e1f2e37ac563`; compute settings unchanged.
Schema: `client_dev.ashlar_stream_20261005_e7`.

This removes the physical serving copy from the overlapping-staging candidate.
Canonical objects, the original property journal and receipt commit atomically.
The immutable manifest records actual object/journal table versions only.
Scalar graph columns and injective node keys are derived by SELECT over the
published canonical `VERSION AS OF`; complete properties and retained content
are retrieved at that same version. No fictional serving-table version is used.
This is a native SQL projection experiment, not an external graph import.

The six scheduled 100k-object batches retain ten-second arrival intervals and
prebuilt producer payloads. One staging session overlaps the serial publisher.
Staging, transaction cardinality/carrier/version/journal-value validation,
version discovery and manifest publication remain in freshness. The prior
bounded id-clustered object fixture and 128MiB target are used, not full
CONTRACT-003 canonical fields or the 16MiB singleton candidate.

| Batch | Processing seconds | Oldest freshness seconds | Newest freshness seconds |
| --- | ---: | ---: | ---: |
| 1 | 21.00 | 31.00 | 21.00 |
| 2 | 15.10 | 36.11 | 26.11 |
| 3 | 14.92 | 41.03 | 31.03 |
| 4 | 16.56 | 47.60 | 37.60 |
| 5 | 13.82 | 51.42 | 41.42 |
| 6 | 16.06 | 57.48 | 47.48 |

All transaction checks and publications succeeded. After the timed run, every
published snapshot returned 100k changed rows with zero exact property/retained/
scalar/version mismatches. Six complete singleton reads matched producer bags
and retained text, and six scalar projections returned the expected injective
node key, source/type/id, promoted values and entity version. These checks are
outside the freshness clock and do not establish scalar query latency or
generic missing/null/cast semantics. All 69 statements succeeded; complete
execution telemetry was refreshed by existing IDs without rerunning operations.

Every batch's oldest freshness is below 60s in this bounded one-minute source
schedule. **Sustained throughput is not admitted:** the last queue delay is
31.42s, later batches require approximately 14–17s against ten-second arrivals,
and median atomic apply alone is 13.29s. Median staging is 5.79s (overlapped),
each version discovery 0.49s and manifest publication 0.85s. A longer run would
need evidence rather than extrapolation; this short-window pass cannot close
the sustained 10k/s gate.

The intervention avoids physical serving synchronization and retains complete
canonical meaning in the tested fixture. It trades that synchronization cost
for query-time scalar extraction, which still needs list/count/two-hop and
connector evidence. PuppyGraph, GraphFrames and bounded Fabric projections
remain unexecuted against this variant. They must not inherit a compatibility
claim from successful native SQL. Fabric's full-graph scale limitation remains.

Next test publication batching at the same 10k/s arrival rate, explicitly
including the larger accumulation window in source-to-publication freshness.
Also compare canonical edge-ID clustering with adjacency-oriented layouts.
Full canonical-schema integration, sustained arrivals, 100k/s burst duration,
reader load, replay/deletes, failure recovery, caller latency, cost and
billion-node/5B-edge admission remain open.

Harness: `SPIKE-001-table-layout/native_stream_projection.py`. Evidence:
`SPIKE-001-table-layout/out/native/ashlar_stream_20261005_e7/`, including
schedule, completed stream summary, publisher/staging statements,
`all-statements.json` and refreshed query history.
