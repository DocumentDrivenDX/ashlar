# Index/carrier singleton disposition R518

Private experiment evidence; no canonical schema or publication changes.

R515 compares complete pinned singleton responses from the wide current table against the index/carrier join. R517 adds explicit hash and full typed-key predicates to the carrier as well as the index. Both use the same eight changed identities: six live updates and two deleted identities. Each executes two passes with one persistent SQL connector and result caching disabled. R515 first-pass order always reads wide before join to capture the paired oracle; second-pass order alternates. Storage cache is already warm; all point queries report zero remote bytes. Each result includes all 20 carrier fields, with timestamp rendered identically as text. This is a small serial sample, not a production latency distribution or cold-data test.

| Cohort, second pass | Caller sample p95 | Engine sample p95 | Eight-query reads |
| --- | ---: | ---: | ---: |
| R515 wide | 452.798ms | 158ms | 515,962,992B |
| R515 index/carrier join | 948.824ms | 488ms | 2,016,141,568B |
| R517 wide | 362.910ms | 90ms | 515,962,992B |
| R517 explicit carrier-key join | 858.891ms | 468ms | 2,016,141,568B |

The additional explicit predicate does not reduce measured read bytes. Query-plan inspection is still needed before attributing the extra bytes to a particular pruning or join strategy. The fixed order and run-to-run compilation/cache differences do not establish a causal latency improvement between join variants. Both join variants fail provisional warm engine100ms/caller250ms gates. The R517 wide sample meets the engine threshold but fails caller; R515 wide misses both. Nearest-rank p95 on eight queries is the maximum observed value and does not establish broad admission.

R518 independently checks all 94 exact native query texts and terminal/final metrics, recomputes sample p95 and read totals, verifies source/code digests, compares every paired full response and deletion absence, and verifies identical opening/closing table UUID/detail/head custody. No tested table head changed. Total cost for both runs is 10,137,396,778 read bytes, zero write and zero spill; elapsed wall clocks are 41.358s and 34.899s, including custody checks and telemetry.

Disposition: retain the proposed wide current table as the default physical candidate. The private split layout remains an ingest-I/O tradeoff, not an accepted singleton optimization. Its lower mutation reads/writes in R513 come with two write phases, extra immutable-carrier retention, cross-table publication/fencing obligations and slower joined reads in this sample. Do not promote it or backfill the larger fixture on this evidence. Native UC Delta remains the architectural commitment, and UMF binding remains deferred. PuppyGraph/GraphFrames/Fabric admission has not changed.

Next useful work is explain-plan inspection of these exact joins and a design decision about consumer-facing current projections versus an immutable carrier/index model. Avoid repeating the same singleton cohort without a material intervention. Actual publication freshness, production p95, cold-data and billion-scale gates remain open.
