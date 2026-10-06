# Full object/journal/manifest schema ingest evidence

Observed 2026-10-05 on dbw-aidev-cus SQL channel 2026.38, existing shared
`data-gateway` warehouse `2439e1f2e37ac563`; compute settings unchanged.
Schema: `client_dev.ashlar_stream_20261005_e9r1`.

The experiment executes CONTRACT-003's object, property-journal and publication-
manifest DDL. The experimental canonical and journal tables add catalog-managed
commit support; objects use source/type/id clustering and a 16MiB target, while
the journal uses the contract's feed/position/id clustering and 128MiB target.
One million canonical objects are seeded from the retained 10M fixture. The
fixture still has one source/type; it does not prove cross-source distributions.

Six 200k-change batches become ready every 20s, preserving modeled 10k/s and
including the full accumulation window. Three 200k-ID ranges repeat twice,
applying 1.2M changes across 600k objects. Prebuilt producer payloads chain
prior values; atomic checks verify exact prior canonical bags and versions.
Canonical state, original old/new property journal and receipt commit together.
Scalar serving is a SELECT at the published canonical version, with no physical
serving copy. Version discovery and the full manifest write remain timed.

| Batch | Processing seconds | Queue seconds | Oldest freshness seconds |
| --- | ---: | ---: | ---: |
| 1 | 27.50 | 0.01 | 47.51 |
| 2 | 21.33 | 7.51 | 48.84 |
| 3 | 20.62 | 8.84 | 49.47 |
| 4 | 18.90 | 9.47 | 48.37 |
| 5 | 24.56 | 8.37 | 52.93 |
| 6 | 18.67 | 12.93 | 51.60 |

All 71 run statements succeeded. Each immutable snapshot subsequently hydrated
200k changed rows with zero exact property/retained/scalar/version/origin/
logical-key mismatches, including older snapshots after repeated updates.
Six complete singleton reads and injective scalar node-key projections matched
producer expectations. A separate read-only verification checked every manifest's
actual table-version vector, profile, source progress, schema revision and
validation report. Journal count and distinct feed/epoch/position/ordinal keys
both returned 1,200,000.

Median atomic apply wall time was 18.49s, staging 6.81s (overlapped), each version
discovery 0.54s and manifest publication 0.88s. Every oldest freshness bound is
below 60s in this two-minute schedule, with maximum 52.93s. Queue delay rises
from startup to 12.93s at batch six, and processing has little margin against
the 20s interval. This is a bounded full-schema pass, not sustained p95, burst,
concurrent-reader, edge-ingest or billion-node admission.

Source fields are a synthetic whole-entity profile (`pilot`, type 1, feed S,
epoch e, revision r1). Property 103 changes; event ordinals are fixture IDs and
source time is explicitly synthetic text. This does not establish native Truss
feed reconstruction or generic missing/null/deletion/replay semantics. Exact
64-bit/decimal/time support and unknown content require their independent
conformance evidence. Catalog-managed atomicity and projection-reader protocol
compatibility remain experimental; external engines are unexecuted.

The initial e9 attempt failed with an authoritative DDL parse error caused by
property-string assembly before any table was created. Its records remain in
`out/native/ashlar_stream_20261005_e9/`. The corrected e9r1 run used a fresh
schema; no timed-out write was blindly resubmitted.

Harness: `SPIKE-001-table-layout/native_stream_full_contract.py`. Completed
run and refreshed same-ID telemetry: `out/native/ashlar_stream_20261005_e9r1/`.
Descriptor and key checks: `out/native/ashlar_full_contract_verify_20261005_e9r1/`.
Next measure native edge identity versus adjacency layouts, longer arrivals and
singleton reads during publication before selecting production settings.
