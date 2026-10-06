# Prebuilt-producer ingest control

Observed 2026-10-05 on dbw-aidev-cus SQL channel 2026.38, existing shared
`data-gateway` warehouse `2439e1f2e37ac563`; no compute configuration changed.
Isolated synthetic schema: `client_dev.ashlar_stream_20261005_e4`.

This repeats the full-carrier catalog-managed atomic publication candidate,
with two deliberate interventions: all producer payloads are constructed before
the arrival clock, and redundant outside-transaction validation is removed.
Timed staging still copies each ready producer batch. Inside the transaction,
cardinality, both canonical/serving property and retained carriers, entity
versions, journal counts, and decoded old/new property values are checked.
Canonical, full serving copy, journal and receipt commit together; exact table
version discovery and manifest publication follow and remain in freshness.

Six scheduled 100k-object batches become ready every ten seconds, modeling
10k changed objects/s for one minute. Arrival clocks persist through backlog.
This is a prebuilt synthetic schedule, not a live producer or long-run p95 test.
No edges, deletes, replay, concurrent readers or recovery are included. It uses
the prior fixture columns with id clustering and 128MiB targets, not the full
13-column canonical table or the 16MiB singleton candidate.

| Batch | Processing seconds | Oldest freshness seconds | Newest freshness seconds |
| --- | ---: | ---: | ---: |
| 1 | 34.98 | 44.99 | 34.99 |
| 2 | 26.24 | 61.23 | 51.23 |
| 3 | 28.53 | 79.77 | 69.77 |
| 4 | 26.71 | 96.47 | 86.47 |
| 5 | 25.04 | 111.51 | 101.51 |
| 6 | 26.75 | 128.27 | 118.27 |

All six transactions and manifests succeeded, along with all preservation
checks. The process completed normally with 49 successful statements and
complete execution telemetry refreshed by existing IDs. Median staging wall
time was 5.89s, atomic apply 17.50s, each version discovery 0.45s, and manifest
publication 0.92s. These component medians are descriptive and not additive
batch percentiles.

The candidate fails <=60s freshness and accumulates backlog despite excluding
payload generation and duplicate validation. Its approximately 25–29s later
batch processing cannot sustain ten-second arrivals. Comparison with the prior
atomic run's 131s final oldest freshness is sequential, not a controlled paired
causal estimate. Synthetic generation was not enough to explain the gap.

Next layout intervention: compare a narrow scalar serving projection carrying
identity/version and explicit canonical residual retrieval, retaining exact
bags and unknown content in canonical state and the original property journal.
That requires an explicit experimental contract amendment and fidelity checks;
it is not permission to silently discard serving content or accept weaker gates.
Publication recovery and representative edge loads remain required.

Harness: `SPIKE-001-table-layout/native_stream_prebuilt.py`. Evidence:
`SPIKE-001-table-layout/out/native/ashlar_stream_20261005_e4/`, including the
schedule, statements, query history and completed stream summary.
