# Actual typed publication response-loss recovery

A fresh isolated installation passed five cases through the installed Python SDK:
initialization, ordinal-one baseline, one interrupted ordinal-two effect response,
ordinary ordinal-two recovery, and ordinal-two replay. Every case retained status
0 and successful owned cleanup. The fault case's status 0 means its private
operator worker verified the intended interruption; it is not a successful initial
publication response.

| Case | Supervised seconds | Outcome |
| --- | ---: | --- |
| initialize | 18.407570917 | Fresh six-table private installation |
| baseline1 | 36.863404125 | Public SDK publication; checkpoint byte offset 931 |
| fault2 | 19.651103333 | Expected one-shot postcommit response loss |
| resume2 | 41.171126709 | Ordinary public SDK recovery; checkpoint byte offset 1493 |
| replay2 | 28.196090000 | Same descriptor; native files and logical journal unchanged |

The private one-shot tracer binds the exact installed transport function and
AST-checked line 948. Only inside actual `LocalDeltaEffects` execution, after the
real Delta SQL result has been collected and commit metadata restored, it records
the exact original native commit and durable SQLite `submitted` intent with null
receipt, then raises one dedicated `OSError` before the owner's receipt/phase
completion. Public cleanup and closing run normally. The postfault witness retains
actual prepared/applying phases, checkpoint **931**, exact request checkpoints,
original intent and native history. No journal repair, reset, replacement intent
or synthetic authority is introduced.

The subsequent unchanged public publication recovers that original operation.
The campaign's read-only final check confirms the same intent/digest/predecessor
now has a normal committed receipt, exact progress offsets **931 and 1493**, and
one native metadata commit for the interrupted operation. The sole matching
metadata log is retained. This is recovery of a committed effect whose original
response was withheld, rather than a simulated-only effect or an invented absent
commit. Byte offsets are not ordinal numbers; the full three-batch source ends
at byte 2051, but this campaign publishes only its first two batches.

Complete native rows/schema are captured only at successful exact owned snapshot
or effect-validation returns, bounded per capture and aggregate. Missing captures
refuse. After public runtime closing, separate actual original-source admission
and the source carrier oracle verify all four bags. Baseline bags contain two
objects, no edges/tombstones and two history rows; recovery/replay bags contain two
objects, no edges/tombstones and three history rows. The typed named fixture and
original producer receipts are retained, including exact native-null/string and
typed numeric/Boolean carriers. Empty tombstone coverage here is not delete
coverage; prior delete evidence remains separate.

Product commit `ddc78d116bc83db04d04a912e8993b4659c716af` was built into an
immutable offline wheel; 497 installed package members match its bytes. The
wheel SHA256 is
`70f171c6f14f503ca50397f2792f67795d5bcf19f7d9800a5e39fd2c560b1002`.
Reviewed-pending and operative selections, exact module/fixture/operator pins,
installed binding preflight and launch clearance are retained. Dependencies use
an explicit link to the previously selected local Spark 4.0.1 / Delta 4.0.0
Python SDK; this is not an independent dependency rebuild.

This qualifies an installed SDK **operator-injected** response-loss scenario and
ordinary SDK recovery/replay over a tiny private immutable-file source. It does
not qualify an uninstrumented CLI crash scenario, remote uncertainty, protected
source ACK, managed Unity Catalog, cloud behavior, throughput or production
security. Each case has its own finite retained-leader supervisor; cancellation
and all-exit cleanup remain owned. The timings are case durations, not latency
budgets or performance targets.

[manifest.json](manifest.json) pins 77 exact deterministic gzip streams
(`mtime=0`), with original/gzip byte counts and SHA256: scripts, selections,
provenance, authored fixture, actual producer/oracle receipts, full witnesses,
public reports, bounded terminal streams, cleanup and replay fingerprints.
[summary.json](summary.json) attributes the parent-reported session/tool result
separately from retained supervisor outputs. No SQLite database, Parquet payload,
binary, SDK corpus or environment dump is archived. Assembly made no native,
source, ACK or process-control calls.
