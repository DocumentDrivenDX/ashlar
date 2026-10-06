# Maintenance-inclusive arrivals with fixed-vector readers

2026-10-05 workspace date, native UTC October 6; SPIKE-001, unchanged dbw-aidev-cus warehouse. Each layout independently receives two common prebuilt 300k-entity batches 30s apart. Logical changed entities cover ten native domains; new property201 carriers are 2KB. LC runs first, partition/Z-order second. Preparation and reader priming are outside the arrival clock; data apply, maintenance, actual snapshot discovery, post-maintenance changed-row validation and guarded publication are inside it. This is a short queue-growth experiment, not a sustained-rate pass.

| Layout / batch | Queue s | Processing s | Oldest freshness s |
|---|---:|---:|---:|
| LC 1 | 0.010 | 74.324 | 104.334 |
| LC 2 | 44.335 | 75.519 | 149.854 |
| Partition 1 | 0.003 | 101.803 | 131.806 |
| Partition 2 | 71.806 | 89.850 | 191.657 |

Oldest freshness starts at the beginning of each explicit 30s synthetic source window. All four observations exceed 60s; processing exceeds the arrival interval and queue grows. Thus these maintenance-before-publication policies fail the bounded 10k/s combined screen. Two failures already contradict admission; a longer passing schedule would still be required for any replacement. Prebuilt inputs and synthetic clocks do not establish consumer source delivery guarantees.

LC atomic-data durations 41.649/40.395s, maintenance 21.794/24.025s, maintained post-check 4.496/4.778s, publication 4.973/4.812s. Partition atomic-data 23.956/24.278s, maintenance 55.405/47.981s, maintained post-check 16.124/11.011s, publication 4.903/5.149s. Other snapshot/resolution/control work is included in processing. Distinct physical histories and sequential order prevent a causal general ranking; the slower partition maintenance is measured in this explicit policy.

One persistent reader per run keeps the old graph's canonical edge snapshot pinned (LC21 / partition3), cycling 51 native keys with full 17-field projection and exact prior-stage equality while their current identities are updated. Reader priming is 51 calls; subsequent calls wait 0.5s between requests. Every result matches the old carrier; no reader can advance by table latest alone. Total reader calls LC199 / partition256. This exercises old canonical rows during full graph publication, not a live consumer's full mixed graph query workload.

Read calls whose actual request intervals overlap first atomic-data start through last publication completion exclude priming and the initial arrival wait. Nearest-rank p95 over all those calls:

| Writer overlap | n | Engine p95 ms | Caller p95 ms | Compile p95 ms | Calls with remote bytes |
|---|---:|---:|---:|---:|---:|
| LC | 112 | 209 | 1,377 | 1,152 | 12 |
| Partition | 168 | 307 | 952 | 644 | 60 |

Zero result-cache hits. Both engine and caller gates fail under the tested overlap. Remote I/O and large compilation durations occur; this is not proof of a specific contention cause or a controlled cold ranking. The all-scheduled-reader summary includes the initial 30s wait and is distinct from the overlap summary.

Each atomic data transaction retains exact full prior and post canonical checks, typed-key counts, complete property journal tokens/origins and durable receipts. A pending barrier spans maintenance, the post-maintenance check and actual-vector publication. All 300k changed canonical/journal checks pass at each publication. Published LC pairs edge23/journal19 then edge26/journal20; partition pairs edge5/journal2 then edge7/journal3. Both preserve node3/adjacency2/degree1/tombstone1 and prior progress/revisions plus fixture-maintained positions1/2. Separate exhaustive untouched/history/descriptor readback and final-barrier checks remain next; in-transaction guard passes do not prove that follow-up or production permissions/recovery.

Harness `SPIKE-001-table-layout/native_maintained_schedule.py lc|partition`, summarizer `summarize_maintained_reader.py`. Complete own query/result records, timestamps, reader records/metrics, progress and terminal summaries under `out/native/ashlar_maintained_schedule_20261005_r40_lc/` and `..._partition/`. No blind retries, shared compute changes, vacuum or UMF binding. Goal active; caller, sustained/burst, full-scale and external-engine gates remain unadmitted.

Next finish exhaustive final verification, then choose a changed maintenance/publication policy or physical layout based on these failures. Repeating the same schedule or counting maintenance as free cannot close the goal.

Independent final verification now passes on both layouts via `native_maintained_final_verify.py`. Each checks 9,419,981 untouched rows across all 17 canonical fields against its preceding graph snapshot, 600k changed/journal records at their own publication and the final vector, 10,019,981 unique total typed identities, exact inherited multiset of 3,120,023 journal rows, 600k unique new origins, both complete descriptors and durable receipts, and cleared owner/epoch barriers at sequence8. Partition bucket derivation also passes globally. Raw queries/results and terminal summaries are under `out/native/ashlar_maintained_final_verify_20261005_r41_lc/` and `..._partition/`. This closes the final verification gaps above while retaining the combined performance failures.

Next physical intervention: a separate **64MiB hash-clustered canonical copy** with the same complete schema, entropy and native predicates. Prior 1M-fixture 16MiB/128MiB evidence leaves this intermediate size untested on the wider 10M fixture. Hypothesis: fewer active files may reduce planning/validation cost, balanced against increased singleton I/O; no benefit is assumed. Capture actual file distribution and first-touch/remote bytes, run singleton reads before exhaustive parity scans warm the copy, then preserve full exact carriers and inspect write/maintenance cost if the read screen warrants it. Keep the 16MiB failed schedules intact. A copy changes row-tracking lineage; hidden identity parity is not a cross-copy claim. Caller, sustained/burst, full-scale and external-engine gates remain open; no resource/compute enlargement is authorized by this experiment.
