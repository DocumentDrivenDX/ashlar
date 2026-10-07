# Ordinary liquid maintenance disposition

Ordinary whole-table OPTIMIZE is not qualified as a cheap incremental maintenance step for the8M-node/39.98M-edge candidate on existing2XSmall compute. Neither the90s nor300s guarded attempt completed. Both are native-final CANCELED and exact GET/history checks show head remains3, with no OPTIMIZE commit. This does not prove the engine cannot complete with more time/compute, nor reverse the lower measured liquid-clustered ingest cost. No further whole-table attempt was admitted.

The first complete400group digest query also canceled at90s before maintenance; it read30,440,870,395 bytes/write0/spill0. Four100group slices then prove all39,980,000 current rows and all20fields at version3, with50,887,108,232 bytes of successful validation reads. That qualification is retained and reused, rather than repeated before the longer attempt.

| Attempt | Native total seconds | Read bytes | Attempted write bytes | Delta outcome |
| --- | ---: | ---: | ---: | --- |
| Whole digest guard90s |100.518|30,440,870,395|0|Read only; head3|
| Ordinary OPTIMIZE guard90s |100.656|93,151,545,455|0|No commit; head3|
| Ordinary OPTIMIZE guard300s |313.657|112,352,429,338|25,252,007,837|No commit; head3|

Guards are checked after the submission response and cancellation takes time, so observed duration exceeds the nominal guard. The longer optimizer reports20.143s cloud-storage retry duration, zero spill, and31.56M attempted output rows. These counters are not durable committed data, retained-storage/orphan inventory, an exact bill or a causal partitioning estimate. Attempted writes remain charged; no cleanup or VACUUM was performed.

Including the successful sliced baseline scans and all inspection SQL, this iteration reads286,831,953,420 bytes, attempts25,252,007,837 writes and spills0. The revised combined500GB-read/80GB-write bounds are explicit. The initial200GB-read phase bound was revised after failure, not retroactively presented as a passing first attempt. Failed source/optimizer handles and native-final telemetry are retained. There is no maintained snapshot and no postmaintenance point result.

## Physical design implication

Retain LC as the proposed canonical baseline given the r337 second-batch39.8s/219MB mutation versus range3260.1s/2.49GB, but keep64MiB a measured tuning candidate rather than a universal default. Publication stays pinned to validated actual versions; physical head changes require preservation validation before any manifest update. A periodic maintenance policy must have independently bounded file selection, work/cost admission, failure custody and preservation proof, outside the ingestion/freshness path. Whole-table OPTIMIZE cannot currently be treated as routine60s cleanup at this scale/compute.

The complete version3 live-file extrema support a smaller next experiment: prefix0 of sixteen hash ranges overlaps49files/2,338,256,957 bytes. Other ranges overlap41–56files/2.34–2.95GB. This is candidate coverage from live extrema, not guaranteed stored-stat eligibility or actual optimizer I/O. Prior r300/r303 demonstrates a separately bounded FULL WHERE prefix rewrite and full-carrier audit on a different private snapshot, including multi-commit handling; it does not prove this snapshot. Next preflight this bounded range, explicitly charge prior attempts, reuse the qualified version3 baseline, perform one guarded range operation and verify complete carrier preservation before matched points. No new range operation executed in r349.

Warm caller250ms/engine100ms, sustained10k/s/100k/s burst, controlled cold and1B/5B admission remain unproved/missed. Truss semantics, permanent raw/journal roles, bounded graph mappings and deferred UMF remain unchanged.

Evidence: `out/lc-maintenance-canceled-disposition-r349.json`, each r341/r343/r347 `audited-cancel.json`, `out/lc-bounded-maintenance-selection-r349.json`; native receipt auditor `lc_maintenance_cancel_audit_r349.py`.
