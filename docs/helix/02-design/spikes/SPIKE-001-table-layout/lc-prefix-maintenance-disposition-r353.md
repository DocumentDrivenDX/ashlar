# Bounded liquid-clustered maintenance and matched reads

The first hash-prefix range on the private39.98M-edge candidate now has successful maintenance evidence, with complete carrier preservation. It reduces sampled read amplification, but warmed singleton latency still misses the provisional100ms-engine/250ms-caller targets. No full-table maintained profile or production policy is admitted.

The qualified version3 live-file selection overlaps49files/2,338,256,957 bytes. One90s guarded `OPTIMIZE ... FULL WHERE lookup_hash < '1000…0000'` returns in33.766 caller seconds. Same-SID commits4 and5 are both accounted: rewrite49files into42, write2,328,325,690 target bytes/remove45 deletion vectors; then a zero-file OPTIMIZE commit. Selected head5 is bound to unchanged UUID2d2672c4-8618-4b10-af9c-477d2c1fcc80. The64MiB target is not an output maximum: largest added file96,093,995 bytes.

Optimizer-only native execution32.774s, reported read7,137,851,228 bytes/write2,328,325,690/spill0;88% cache-byte fraction, remote840,614,289 bytes. These counters are not the2.338GB physical-selection size or unique I/O. Complete400 bounded100k-identity-group20-field before3/after5 multisets agree for all39,980,000 rows, including exact props/retained/cursor strings, typed identities/endpoints, versions and origin. SHA256collision resistance assumed. Version3 baseline is reused from the fully native-final r343 slices; no before scan replay. All9new statements are successful/native-final and after digests uncached. Whole phase348.681s, read63,086,876,188 bytes/write2,328,325,690/spill0. Most controller time is exhaustive experiment verification, not optimizer execution.

Matched64point cohort is identical to unmaintainedLCr339:4second-deleted12second-updated16unchanged identities, repeated twice. Complete20fields or exact absence pass, all68native statements final and point results uncached. Version5 has600live files versus607at3. No source mutation/new logical publication.

| Metric | Before3 | After5 |
| --- | ---: | ---: |
| Combined caller p95 ms |579.137|548.603|
| Combined engine p95 ms |283|259|
| Warm second-pass caller p95 ms |435.132|416.054|
| Warm second-pass engine p95 ms |101|104|
| Combined read-byte p95 |162,528,541|144,716,272|
| Combined file-count p95 |5|2|
| Queries with remote reads |8|6|

First-pass after maintenance caller823.727/engine504ms; small inside-prefix cohort has4queries across both passes and caller905.459/engine626ms. It is not a service tail or cold qualification. Full audits and metadata scans warm/evict cache; before/after phases have different remote fractions. Lower sampled bytes/files do not establish causal latency improvement. After-point total reads7,856,591,061 bytes, write0/spill0.

Cumulative canceled+successful maintenance family costs:349,918,829,608 bytes read and27,580,333,527 bytes attempted written,0spill. Including follow-up reads:357,775,420,669 bytes read. Prior25.25GB canceled writes are charged despite no commit; no orphan inventory/cleanup/VACUUM or retained-size/bill claim.

## Design disposition and next step

Retain unpartitioned LC as the proposed canonical physical baseline and keep exact native identity predicates alongside the derived lookup hash. Treat bounded prefix maintenance as a separately scheduled, admitted operation with actual file coverage, byte/time bounds, commit custody and preservation qualification. Prefix1/16 is this experiment's range, not a universal1B/5B policy or cadence. Whole-table maintenance remains unevidenced within prior guards. No existing manifest is repointed; exposing a maintained version requires the governing publication/read protocol.

Caller latency remains the open singleton issue. Next use a small native result-cache/transport experiment to distinguish repeated exact pinned lookups from uncached physical reads, retaining full identity and20-field/absence checks and reporting hits/misses separately. A cache result must not be presented as improved cold/physical pruning or arbitrary1B-key working-set admission. The optional owner calibration question compares retaining250ms versus500ms on this compute; until answered,250ms remains the target. No target revision, sustained10k/s/100k/s, real producer/concurrent fence, graph-engine interoperability or billion admission is inferred. UMF remains deferred.

Evidence: r350/r351 audited maintenance and r352/r353 audited point receipts in `out/native/`; earlier failed work remains r349.
