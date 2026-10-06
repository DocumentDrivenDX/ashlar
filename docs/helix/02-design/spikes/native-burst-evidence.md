# Provisional ten-second 100k/s burst evidence

Observed 2026-10-05 on dbw-aidev-cus SQL channel 2026.38, existing shared
`data-gateway` warehouse `2439e1f2e37ac563`; compute settings unchanged.
Schema: `client_dev.ashlar_burst_20261005_k1`.

One million changes accumulate over ten seconds, modeling 100k changes/s. The
ten-second duration is an explicit comparison assumption, not owner-approved
burst duration or production p95 evidence. Input is prebuilt, but the full
accumulation window, timed staging, validation, version discovery and manifest
publication are included in freshness. No concurrent reader load is added.

The full CONTRACT-003 object/journal/manifest candidate uses 16MiB source/type/id
object clustering, feed/position/id journal clustering, and experimental catalog-
managed canonical/journal/receipt atomic publication. Scalar serving derives
from the pinned canonical version. Source identity/type/feed are the same single
synthetic profile as prior tests. Old payloads contain 600k varied and 400k
repetitive bags; all new property-103 values use varied 2KB SHA hex payloads.
This mixed old-payload distribution is not independent per-node production
entropy or native Truss feed reconstruction.

| Timed component | Caller wall seconds | Engine seconds |
| --- | ---: | ---: |
| Stage | 11.26 | 10.83 |
| Atomic apply and validation | 47.22 | 46.75 |
| Object version discovery | 0.98 | 0.28 |
| Journal version discovery | 0.41 | 0.20 |
| Manifest publication | 0.78 | 0.57 |

Total ready-to-publication processing is 60.63s. Newest-change freshness is
60.64s and oldest-change freshness 70.64s including accumulation. Even the
newest modeled change exceeds 60s, so the candidate does not admit this burst
scenario. One observation cannot establish population burst p95 or rule out
other batching/compute configurations.

All prior-state, canonical value/version, retained-content, journal count and
decoded old/new checks pass. After publication, hydration returns 1,000,000
changed rows and zero mismatches across exact bags, retained text, logical keys,
origin fields, promoted scalar values and entity version. Complete singleton
and injective scalar projection checks also pass. All 21 statements succeed;
metrics were refreshed by existing IDs without repeating writes. Post-run
hydration checks are outside publication timing; the inside checks remain timed.

This preserves the tested synthetic meaning but fails the provisional freshness
budget for a ten-second burst on current compute. The previous two-minute 10k/s
pass cannot substitute for burst admission. Next test edge publication and
review explicitly bounded compute isolation/scaling; billion-node scale and
caller latency remain unresolved. No compute setting or performance gate was
changed to obtain this result.

Harness: `SPIKE-001-table-layout/native_burst.py`. Evidence:
`out/native/ashlar_burst_20261005_k1/`, including schedule, completed stream
summary, combined statements and refreshed query history.
