# Full range32 physical comparison: r310/r311/r321

Governing ADR-001/CONTRACT-003 and exact CONTRACT-001/002 publication/read
boundaries remain. The whole39.99M-edge range32 comparator has complete20-field
and final global integrity proof; all32partitions underwent ZORDER. UMF remains
deferred and direct graph-reader feature support remains separately qualified.

All64sequential and32concurrent exact full-carrier/deletion checks pass. Range32
sequential read-byte p95 is79,482,625 and read-file p95 is1, versus188,283,489/3
in the previously maintained liquid-clustered cohort. Combined range32caller/
engine p95 are703.517/444ms, including23queries reporting remote data bytes.
First32pass has22remote queries and786.185/515ms caller/engine p95. Second32pass
has1remote query and366.780/96ms caller/engine p95. Observed remote subset23
queries has786.185ms caller p95 and786.426ms maximum, but preceding full audits
and metadata scans mean this is not a controlled universal cold-data result.

Two-reader same32key cohort measures395.210ms caller/104ms engine/214ms compile
p95,79,482,625read-byte p95 and1filep95. Prior liquid-clustered2reader cohort
measured624.382/103/412ms caller/engine/compile p95 and188,283,489bytes/3files.
Native request intervals establish overlap; ordered different runs/cache states
prevent causal speedup claims. No compute changed; both result caches disabled.

Hard partition pruning plus within-partition ordering now has fullscale-of-spike
read evidence, while warmer second-pass engine p95 reaches100ms in the scoped
sample.250ms caller remains missed; concurrentengine104ms also misses100ms.
Don't select on the warmed engine-only result or claim robust service tails.
The next required comparison is ingest/publication of the distinct second batch,
including raw/journal/retained preservation, complete clock and any maintenance.
The previous100k batch139.786seconds remains the freshness evidence; this read
comparison does not replace it. Node layout, repeat batches, controlledcold,
realproducer fencing and1Bnode/5Bedge capacity remain unqualified.
