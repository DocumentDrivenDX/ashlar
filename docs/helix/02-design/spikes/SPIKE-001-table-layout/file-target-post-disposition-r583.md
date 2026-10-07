# Changed-key reads and file-target decision — R581–583

After matched full-guard ingest, the same persistent SQL client reads six updated
and two deleted fourth-batch identities at actual64MiB fixture1 and256MiB
fixture4. Two passes alternate version order. Every response includes all20
carrier fields or exact absence for deletion. The independent synthetic oracle
reconstructs each fourth-batch identity, exact tokens/cursor/batch marker/endpoints
and expected lifecycle state. All32 responses and43 exact native statements/
final metrics pass audit; complete closing two-table UUID/profile/head custody
is unchanged and result cache is disabled.

| Cohort | Caller p95 ms | Engine p95 ms | Compile p95 ms | Read bytes |
| --- | ---: | ---: | ---: | ---: |
|64 first|367.345542|92|164|515962992|
|256 first|373.145917|87|168|1007788839|
|64 repeat|397.577959|94|184|515962992|
|256 repeat|420.603625|111|180|1007788839|

Every read reports zero remote bytes. Both layouts read median2 files, including
post-ingest geometry;256 reads95.32% more bytes. Total3051992931 read B,0 writes/
spill,25.506035042 seconds within20GB/0/0 and180-second bounds. Eight samples'
nearest-rank p95 is their maximum, not a production tail estimate. These selected
changed keys are intentionally a different workload from R574 spaced unchanged
keys; do not average the cohorts into an asserted global distribution. No
controlled cold-data or writer-overlap performance is claimed.

**Disposition:** Retain64MiB as the default candidate. Do not promote256MiB from
the lower single MERGE timing or its pre-ingest singleton sample. The matched
256 MERGE used5.553s engine versus7.232s at64, with nearly identical read bytes;
this changed-key sample increases read bytes and misses100ms engine in repeat,
while both fail250ms caller. Prior256 maintenance adds21.108s engine/1.504GB
writes. Cache/order, different compiled snapshots and one mutation per fixture
prevent causal or amortization claims. The measurements justify rejecting
promotion, not proving256 is universally inferior on other compute/workloads.
Generated-column fixture mapping support remains unqualified.

The smaller-file candidate's scoped94ms engine repeat does not close caller,
controlled cold-data, concurrent publication, sustained10k/s,100k/s burst or
1B-node/5B-edge admission. The canonical UC Delta choice and exact Truss meaning
remain fixed; UMF remains deferred. The latest full40M-current publication is
still221.83s and the latest full-current MERGE64.68s. These2.5M-row physical
experiments do not replace those whole-graph observations. Next prioritize a
bounded complete publication integration or explicit compute/capacity comparison
rather than repeat the256 policy unchanged; any resource-size change still
requires its existing pending authorization.
