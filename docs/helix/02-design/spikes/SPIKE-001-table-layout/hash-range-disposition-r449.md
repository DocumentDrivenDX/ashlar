# Exact scattered-change range occupancy (r449)

Goal: determine whether identity hash-range scheduling can reduce total target coverage for the existing qualified100k scattered-change workload before authorizing another mutation. Baseline is the r447 full-parent scan. Intervention is grouping exact hashes into equal byte ranges; stop after one100k local pass,180s maximum, no native writes.

Executed9 seconds on the exact FourthChanges generator:100k unique hashes cover all16,64,256,1024 and4096 ranges. They cover51,321/65,536 ranges at16bits. These are uniform mathematical ranges, not measured Delta file boundaries.

At64ranges,100-row arrival batches touch79.23% of ranges on average;1000-row and10k-row batches touch all ranges. At1024ranges,10k-row batches touch99.95%. Independent microbatch scans therefore revisit most target coverage rather than avoid it. Grouping the whole batch once by range can bound each query's working set, but covers the whole target and introduces multiple query/commit/publication coordination costs. It does not establish a60s freshness solution or justify explicit partition directories.

Do not multiply single-range runtime by64 and claim capacity: initialization/cache/concurrency and file overlap differ. Next native read-only pair tests one1/64 source range with and without an explicit target hash-range predicate, preserving all20 predecessor fields. The native experiment tests pruning mechanics only; any later scheduler must measure full-batch costs, queue time, source order and six-role publication correctness on fresh changes.

Source/code SHA, exact hash sequence SHA and all batch occupancy counts are in out/hash-range-occupancy-r449.json. No real consumer data, canonical role write or UMF binding occurs.
