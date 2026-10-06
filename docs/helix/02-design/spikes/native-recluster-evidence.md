# Incremental reclustering and singleton pruning

2026-10-05; SPIKE-001 bounded native control, not layout approval.

`native_recluster.py` runs one incremental OPTIMIZE on the private n1 10M-node canonical table on existing dbw-aidev-cus PRO 2X-Small compute. Before version **19**, 624 files / 8,286,009,303 bytes. The explicit maintenance statement took **69.05s wall / 67.95s execution**, reading 8,874,378,096 bytes, writing 6,342,959,819 remote bytes and scanning 10,884,265 rows; no spill reported. Its resulting snapshot is **22**, 628 files / 7,640,979,683 bytes. No VACUUM or retention change occurred. Maintenance is separate from prior publication timing, not silently charged to zero.

The same 51 keys and six-column carrier projection as n6 run at version 22 in prime/repeat/repeat2 phases. All returned carriers match recorded version-19 results exactly. This is sample preservation evidence for 51 keys, not a new exhaustive 10M-row maintenance comparison. Rep zero is excluded, leaving 50 samples per phase; result cache hits are zero. Telemetry was refreshed for the same 160 completed statement IDs without rerunning queries.

| Phase | Engine p95 ms | Caller p95 ms | Remote-reading samples | Median files read |
| --- | ---: | ---: | ---: | ---: |
| Prime | 379 | 643.0 | 50 / 50 | 3 |
| Repeat | 83 | 371.0 | 0 / 50 | 3 |
| Repeat2 | 105 | 410.5 | 3 / 50 | 3 |

All samples read **2–4 files**, median pruned **625**; read-byte p95 is ~24.32MB. The preceding identical lookup shape at version 19 read 65–67 files and ~1.106GB at read-byte p95. This controlled key/projection comparison supports a pruning improvement after maintenance; shared-compute/cache conditions prevent attributing every latency difference solely to clustering.

For explicitly **no-remote** samples: Repeat n=50 has engine p95 **83ms**, caller **371.0ms**; Repeat2 n=47 has engine **84ms**, caller **367.2ms**. These pass the provisional warm engine screen but fail 250ms caller latency. The unfiltered Repeat2 105ms includes three remote-reading samples and must not be presented as a fully warm phase. Prime's 50 remote reads and 643ms caller p95 pass the bounded remote-read screen under 1s, not a cold population or billion-scale claim.

Raw run: `SPIKE-001-table-layout/out/native/ashlar_recluster_20261005_n7/`; harness and regenerated client summary are retained. No continuous maintenance policy, concurrent ingest/reader performance, production native source feed, replay/deletion/recovery or 1B-node/5B-edge performance is proved.

## Disposition

Pruning can be restored, but the measured 69s maintenance pass cannot simply be ignored when selecting a layout that must ingest at 10k/s and serve low-latency lookups. It does not by itself prove maintenance can keep pace, nor establish per-batch cost. Next investigate documented write-organization controls or partition/bucket alternatives, then measure a scheduled write/read workload and maintenance together. Keep exact identities, full property/retained carriers, journal values and version vectors. Caller latency and burst/scale/cost gates remain open. The overall goal remains active.
