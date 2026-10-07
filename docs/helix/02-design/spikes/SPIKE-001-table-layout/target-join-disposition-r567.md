# Range target-access pilot — R565–567

This read-only pilot uses the sixth input's before-carriers and the actual
pre-sixth edge snapshot8. Source and target both filter lookup_hash in[00,04),
a1/64 hash-domain interval. The source bound proves1507 unique full typed keys
and7813879 original input JSON UTF-8 bytes, below2500/16MB limits. This byte bound
is an input-text bound, not executor broadcast-memory admission. Every query
checks all20 predecessor fields using the established native null-safe comparisons
and exact UTF-8 string tokens. All1507 match, with0 missing/mismatched carriers
in each of four ordered default/broadcast/broadcast/default observations.

Caller ms:3265.983/2409.284/2626.802/2499.981. Engine ms:2748/1925/2090/1984.
First default read927852100 bytes,304695322 remote. The remaining three each
read927290388 bytes, all reported from data cache. Every query reports32 files
read and590 pruned; these are query-wide counters, not distinct target coverage.
Whole bounded run30.908109917 seconds,4026606654 read bytes,0 writes/spill.
Nine exact native statements, full result/plan hashes, finality and metrics pass
independent offline audit. No table data or descriptor changed.

Both initial plans contain SortMergeJoin LeftOuter. BROADCAST(s) did not produce
a broadcast plan: s is the preserved side of this left outer join. Do not claim
an executed broadcast comparison or attribute the first default's slower timing
to the hint; cache/order differ and the last default matches the hinted timings.
A full target broadcast is outside this bounded pilot. The correct disposition
is to reject promoting this hint from these observations, not to remove missing-
predecessor detection or alter join semantics to obtain a favorable plan.

Explicit range filtering prunes most files in this one interval, yet the full
100k scattered batch covers the whole hash domain. Prior64-way subdivision
(R456) increased total time and I/O; this one-range observation does not overturn
that whole-batch evidence. Do not multiply or divide the pilot latency to claim
full-batch admission. Initial plans are not the executed adaptive MERGE plan.

The material path remains a matched physical-layout pilot with full-source
predecessor protection and preparation/maintenance charged to publication, or
an explicitly bounded resource comparison if the owner approves it. Repeating
unchanged batches or ineffective hints is not justified by these results.
Existing64.68-second current MERGE and221.83-second full publication remain the
latest whole-batch observations. Warm singleton and actual sustained/burst and
1B/5B performance remain unproved. UC Delta, exact Truss semantics, external
mapping limits and deferred UMF binding remain unchanged.
