# Two-reader full-carrier disposition: r307

Governing ADR-001/CONTRACT-003 and CONTRACT-001/002 boundaries are unchanged.
Two persistent SQL connections submit synchronized pairs over the same32
stratified singleton keys used by r301 at maintained version4: deleted, updated
and unchanged identities. All32complete20-field/exact-absence assertions pass;
36native statement receipts are final and all32point results are uncached.
The audited native request intervals establish actual overlapping requests,
including planning/queue lifetime, not proof of simultaneously executing scans.

Caller/engine/compilation/native total p95 are624.382/103/412/517ms. Read bytes
p95 remain188,283,489 and read files p95 remain3. Total4,413,520,502bytes read,
zero writes and zero reported spill. Existing compute remains2XSmall; no resize.

Compared with the prior single-connection64query cohort (caller385.014ms,
engine111ms, compilation170ms), these measurements suggest planning contention
merits investigation. Different cohort length, cache/startup conditions and
separate runs prevent attributing the difference causally to concurrency alone.
Targets are unmet in this sample. This is neither a throughput load test nor
concurrent-ingest freshness/correctness evidence; no production writer fence,
controlled cold, robust service tail or billion-node admission is established.

The next physical candidate must be evaluated under both one and two readers,
with complete carriers and matching keys. Avoid choosing solely from engine
latency or favorable maintained-range keys: caller planning cost is material.
