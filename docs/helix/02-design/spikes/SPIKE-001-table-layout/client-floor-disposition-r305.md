# Persistent-client calibration disposition: r305

Governing design remains ADR-001/CONTRACT-003 and CONTRACT-001/002 publication
and read boundaries. This is a scoped native experiment on the previously
preservation-qualified private maintained version4, not a new architecture.

Sixteen trivial parameterized literal queries alternate with sixteen exact
full20-field singleton queries through the same persistent SQL connection.
The8stratified unchanged keys repeat twice; all checks pass, and all32calibration
queries are uncached with final native history. Total query count34 includes
session timeout/cache settings. No table writes or compute changes occur.

| Measurement p95 | Literal | Full-carrier point |
| --- | --- | --- |
| Caller |176.841ms|421.509ms|
| Native total |75ms|313ms|
| Engine |27ms|92ms|
| Compilation |38ms|215ms|
| Read bytes |0|135,662,939|
| Caller minus paired native total |111.399ms|120.390ms|

The signed paired residual includes client/network/fetch/poll and differing
clock boundaries; it does not isolate network latency. Component p95 values
cannot be added. The literal baseline shows considerable caller overhead even
without Delta reads, while the point sample also spends substantial time in
compilation. These small cohorts are descriptive; different keys from the full
64query cohort do not qualify the global100ms engine or250ms caller target.
Cold/concurrent/sustained service and billion admission remain unproved.

Reducing per-point file bytes is still useful, but insufficient as the sole
workstream for250ms caller latency. Next inspect reusable statement/compiler
behavior and metadata planning on identical pinned versions without changing
identity predicates or dropping carrier fields. Keep parameterized native
singletons independent of Fabric. Any new narrow physical projection must
preserve/rejoin exact property and retained values and be measured end-to-end;
a faster key-only query cannot substitute for the requested full carrier.
