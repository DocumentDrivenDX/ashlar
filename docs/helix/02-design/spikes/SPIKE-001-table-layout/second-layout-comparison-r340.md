# Second-batch physical layout comparison

**Proposed disposition:** retain unpartitioned liquid clustering by lookup_hash as the primary physical design baseline. Fixed32 contiguous-hash partition/ZORDER is an experimental serving alternative, not the canonical default or billion-scale partition count. Both remain short of provisional performance targets. Unity Catalog Delta and full Truss-compatible carriers remain selected; UMF binding and graph-engine admission stay deferred.

Both runs use the exact same immutable second100k inputs:90k updates/10k deletes, complete20-field190k change-image oracle and identity/version predicates. LCr337 reconstructs first batch from original40M qualified clone, proves complete first190k images,39.99M count and all100k exact second predecessors before second apply. Range32r328 clones independently built/fully32partition-maintained first-batch version52. Reconstruction and physical shape therefore differ; runs are sequential with different cache/remote fractions. No causal speedup or SLO-tail claim.

| Second mutation | Liquid clustering | Range32/ZORDER |
| --- | ---: | ---: |
| Caller seconds |39.792|60.119|
| Native engine seconds |39.000|59.070|
| Read bytes |34,754,294,878|36,621,323,047|
| Written bytes, including CDF |219,377,048|2,492,506,686|
| Remote read bytes |10,512,061,874|18,451,338,084|
| Copied neighboring rows |0|2,844,909|
| Target files added |2|32|
| Target bytes added |72,508,855|2,345,489,472|

LC uses64MiB target and explicit write-time CDF. Clone0/config1/firstMERGE2/secondMERGE3 all have actual SID history. Whole experiment177.854s includes123.430s reconstruction/qualification preparation and two MERGEs, read103,359,182,050 bytes/write438,938,492/spill0. Second complete-image validation5.773s. All19 statements native-final, full first and second images plus predecessor checks audited. No permanent R/J extension or whole second graph publication is inferred from this edge-only comparator; the separate full range32 publisher supplies scoped publication evidence.

Same64post-second-batch read cohort on each:4deleted12updated16unchanged identities across40M, repeated twice, exact complete carriers/absence, uncached native-final telemetry. No postwrite maintenance. Live counts39.98M; LC607 live files versus range32448.

| Singleton metric | LC | Range32 |
| --- | ---: | ---: |
| Combined caller p95 ms |579.137|857.676|
| Combined engine p95 ms |283|556|
| Second-pass caller p95 ms |435.132|481.936|
| Second-pass engine p95 ms |101|134|
| Combined scan-byte p95 |162,528,541|148,147,618|
| Combined file-count p95 |5|2|
| Queries with remote bytes |8|25|

Different temperature and prior scans limit latency comparison. LC scan bytes/file counts are slightly worse; its sampled latency is lower, but warmed caller250ms and engine100ms targets still miss. Neither is a controlled all-cold/service percentile,10k/s sustained/100k/s burst or1B/5B-scale qualification. Full range32 publisher mutation90.2s/processing243.8s including failure recovery already misses60s; no full LC second publication latency was measured here.

**Next:** measure bounded ordinary incremental LC OPTIMIZE after these two batches, preserving all20fields and exact identities/versions, then repeat this identical point cohort. Include maintenance writes, failed-attempt costs and retained-file implications. If incremental maintenance cannot improve native singleton cost cheaply, spike a separate Delta serving profile without weakening canonical history/publication semantics. Avoid promoting fixed32 partition count to projected multi-TB storage or changing provisional targets silently.

Evidence: `out/native/ashlar_lc_second_apply_r337/audited-summary.json`, `out/native/ashlar_lc_second_points_r339/audited-summary.json`; comparators r328/r330. Offline auditorsr338/r340. No production configuration, pointer, ACK, native graph integration or compute resize.
