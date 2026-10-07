# Range32 second-batch ingest and read disposition

The fixed32 contiguous-hash partition plus64MiB/ZORDER candidate remains experimental. A private clone of qualified39.99M edges at version52 accepts a second distinct100k scattered change batch correctly, but measured mutation and post-ingest singleton latency miss provisional targets. Unity Catalog Delta remains the selected architecture.

## Mutation evidence

`range32_second_edge_current_r328` UUIDd4d77014-61a7-4e2b-b0ca-a0a78c8ddca4 has clone0, explicit write-time CDF configuration1 and MERGE2. The MERGE includes physical partition equality, full lookup hash, original source/type/id and version1/bootstrap predecessor. All14 native statements are final/successful and exact SQL matches their receipts. All190k20-field change images match independent second-batch oracle:90k preimages,90k postimages,10k deletes, no inserts. This changed-image proof does not replace a global postapply identity/endpoint sweep or full graph publication.

Mutation caller60.119s, native execution59.070s; full-image read4.753s. Whole clone/configuration/mutation/image experiment93.220s. Native totals36,621,487,958 bytes read,2,492,506,686 written,0 spill. Explicit partition predicate presence does not itself prove effective batch pruning. MERGE copied2,844,909 neighboring rows, rewrote32 files with2,345,489,472 target bytes added, added416 deletion vectors and32 change files. Scan8.541s, rewrite47.122s according to operationMetrics. This one controlled batch is neither10k/s sustained nor100k/s burst evidence. Mutation alone exceeds60s caller budget; full publication freshness remains unmeasured for this layout.

## Post-ingest singleton evidence

Pinned version2 has39,980,000 live edges in448 live files, unchanged file count from the parent. No postapply maintenance occurred. The cohort has4second-deleted,12second-updated and16unchanged identities spread across40M, repeated twice. All64 full20-field results/exact absences pass with uncached native-final telemetry. Changed identities differ from the earlier first-batch cohort, so differences are descriptive rather than matched causal estimates.

| Cohort | Caller p95 ms | Engine p95 ms |
| --- | ---: | ---: |
| Combined64 |857.676|556|
| First32 |889.522|582|
| Second32 |481.936|134|

Combined scan p95 is148,147,618 bytes/2 files; compilation221ms;25queries report remote reads. Total read7,320,948,524 bytes, write0, spill0. Metadata scan and preceding change audit affect temperature; these are not controlled all-cold or service-tail guarantees. Second-pass warm targets100ms engine/250ms caller are missed. Remote-byte queries do not by themselves establish the cold-data1s gate.

New MERGE target files span almost a complete1/32 hash partition, while inherited ZORDER files are narrower. File count alone therefore hides pruning deterioration. Maintenance cadence and complete maintained ingest cost must be measured before selecting this as the serving profile.

## Next work

Complete the six-role private publication fixture with first-batch baseline custody and the same second input pins, exact append/history digests, global unique identities/deletion absence/typed endpoint closure and descriptor readback. Measure whole processing time, including validation and telemetry. Run the same second-batch mutation on a qualified liquid-clustered comparator and report cache/shape differences. Then qualify post-ingest range32 maintenance preservation and include its cost and follow-up reads. Do not promote fixed32 partitions to billion scale:32 becomes too coarse at the projected14.17TB active graph and adaptive/larger bounded partition counts remain open. No graph-engine admission, real producer fencing/ACK, concurrent writer recovery or UMF binding was added.

Evidence: `out/native/ashlar_range32_second_apply_r328/audited-summary.json`, `out/native/ashlar_range32_second_points_r330/audited-summary.json`, `out/second-cdf-image-oracle-r327.json`; offline auditorsr329/r331.
