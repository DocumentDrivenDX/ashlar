# 250k schedule and post-ingest singleton evidence

2026-10-05; SPIKE-001 experiments, no layout approval.

## 250k/25s schedule

`native_scheduled_broadcast250k.py` keeps modeled 10k changes/s while reducing accumulation to 25s, with four disjoint 250k modular sets (selector range 1.8M..2.8M) spanning the full 10M-node domain. Independent staging and serial broadcast validation remain unchanged, including exact prior/current/journal checks and total/distinct identity cardinality. Producer generation is excluded; staging, queue, validation, writes, version discovery and manifest are timed. Existing PRO 2X-Small warehouse settings remain unchanged.

| Position | Queue delay s | Processing s | Oldest freshness s | Canonical/journal versions |
| --- | ---: | ---: | ---: | --- |
| 8 | 0.005 | 35.47 | 60.47 | 16 / 8 |
| 9 | 10.475 | 27.66 | 63.14 | 17 / 9 |
| 10 | 13.136 | 27.51 | 65.65 | 18 / 10 |
| 11 | 15.648 | 25.22 | 65.86 | 19 / 11 |

All four miss 60s. Queue grows despite staging overlap. Atomic wall durations are 29.24, 25.85, 25.24, 23.40s; cache percentages 45%, 90%, 92%, 92%. Script aggregate reads are 15.54–19.08GB and rows scanned 40.70–42.95M, with no spill reported. This is a short schedule, not sustained/p95 capacity evidence.

The harness captured before version **12**, 507 files / 8,361,260,310 bytes. History also records OPTIMIZE versions **13–15** before the first transaction at 16: incremental clustering and deletion-vector materialization changed physical state. These operations are absent from this harness's submitted statement records; their initiator is not established by this report, and history labels auto=false. Do not attribute the timing difference solely to batch size, assume an optimization policy, or assume maintenance was included in publisher service time. Logical prior checks remain against exact staged state.

All four published-vector changed/current/journal checks pass for 1M changes; final **9M untouched rows match all 13 canonical columns** against version 12. Final canonical identities: 10M distinct typed tuples. Journal events for this schedule: 1M distinct origin keys. All 42 statements succeed and the harness completes. Post detail: **624 files / 8,286,009,303 bytes**. Raw run: `out/native/ashlar_scheduled_broadcast_20261005_n5/`.

## Singleton after scattered writes

`native_post_ingest_reads.py` pins canonical version **19**. Fifty-one keys span the 10M range, including 16 changed and 35 unchanged; prime/repeat/repeat2 each run the same exact-carrier singleton query. Every result has the expected identity/logical key; changed fixture versions and source positions match; repeated returned carriers are identical. Result cache is disabled. Rep zero is excluded, leaving 50 samples per phase.

| Phase | Engine p95 ms | Caller p95 ms | Median files read | Remote-reading samples |
| --- | ---: | ---: | ---: | ---: |
| Prime | 311 | 575.0 | 66 | 3 / 50 |
| Repeat | 127 | 440.1 | 66 | 0 / 50 |
| Repeat2 | 117 | 391.8 | 66 | 0 / 50 |

Warm repeats fail both provisional singleton gates (100ms engine, 250ms caller). All measured remote-byte fields are present; both repeat phases have zero remote bytes and zero result-cache hits. Files read range **65–67**, median pruned **558**, reported read-byte p95 **1,105,628,142** for every phase. Contrast with the earlier clean-table one/two-file screen is descriptive, not a paired causal estimate: selected keys, projection and canonical feature/maintenance states differ. It establishes that this mutated published layout itself does not satisfy the read target.

Telemetry was refreshed for the same 154 completed statements; no singleton queries were rerun to fill missing metrics. Raw run and summary: `out/native/ashlar_post_ingest_reads_20261005_n6/`. These are post-publication reads, not concurrent-write latency or population-p95/1B-scale evidence.

## Next decision

Measure bounded reclustering cost and repeat this pinned-key read shape on its resulting version. Maintenance must be considered alongside publication and lookup rather than silently excluded to claim a layout meets both targets. If reclustering restores pruning but cannot keep up with ingest/freshness, compare write organization or partition/bucket candidates with exact identity and preservation retained. Do not infer billion-scale performance from cached 10M data, or further tune batch size while ignoring file growth.

Native Truss feed/replay/deletes/recovery, catalogManaged Beta compatibility, concurrent-reader singleton targets, 100k/s burst, 1B nodes/5B edges, external graph-engine protocols and resource/cost bounds remain open. The full goal remains active.
