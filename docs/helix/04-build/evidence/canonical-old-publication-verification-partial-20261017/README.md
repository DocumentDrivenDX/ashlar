# Canonical old-publication verifier: incomplete attempt

One actual bounded `verify-commerce-evolution-publication` invocation selected ordinal 0 of
the retained fresh `native-r2` lineage, after slots 0 and 1 were completed. It
selected the explicit `canonical-managed-carriers/0.1` schema profile and original
run SHA256 `6e466770c8973b3c3104b48924c2e3e4c954745e63c054e0a14817299bc674ed`.
This was a verification attempt, not source append, ACK submission or publication
continuation. Earlier partial publication and protected ACK observations remain
[separate evidence](../current-combined-source-b1-partial-20261017/README.md).

The exact installed Ashlar revision was
`c370255b61bea622625f5123a4ecc87bb0408bb4`, wheel SHA256
`6d82e4c6f614025c3bbc15b65423f817c19fee3cf8a5d96cfbffe721bfe6805d`.
Preparation SHA256 is
`267116cb93a0e95eed3c3d39574a1e6df021406c4d0ae2a94c4cd5483e76604d`.
The preparation and execution-clearance records are retained distinctly: readiness,
CI success and independent review did not establish verification success.

The verifier reached its 180-second work deadline and released no
`ordinal-0-verification.json` success receipt. Its original terminal reports
`TimeoutError`, null child status, `cleanup_confirmed: false`,
`cleanup_failed: true` and `complete: false`. After kill/reap, the original
absence check failed with `PermissionError` for recorded group `58377`. The
later elevated read-only process observation found no rows for that exact group.
It is transcribed with its original tool-output reference and does not repair
the original cleanup result or authorize signals after reaping.

A separately transcribed read-only ledger observation after this verifier and
before the next A2 attempt still showed slots 0/1 completed and 2–7 unprepared.
It contains selected states, not a SQLite byte snapshot or full native-content
comparison. It supplies no additional ACK or verification result. These records
must not be interpreted as a successful canonical historical read, completed
full-prefix verification or proof of state after the separately selected A2 run.

The archive retains exact controls, configuration, provenance, bounded
stdout/stderr and original failure records. Spark `WindowExec` warnings do not
establish the timeout's cause. Only the private diagnostic's byte length and hash
are retained; raw stack, operator environment, DSNs, credentials, binaries and
bulk Delta/SQLite payload are excluded. No causal performance profile, stage
latency, throughput, cloud or production-authority claim follows.

`manifest.json` binds original and compressed SHA256 digests and lengths for
19 exact streams; gzip timestamps are zero. The assembler made no live
native/database/source/process calls, retry or tests. A separately reviewed A2
control and its execution are outside this failure record.
