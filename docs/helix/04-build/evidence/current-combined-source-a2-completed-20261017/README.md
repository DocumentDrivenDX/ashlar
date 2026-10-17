# Closed combined-source A2 prefix

One installed `continue-commerce-evolution` invocation successfully resumed the
original retained A2 attempt in the fresh `native-r2` lineage. The public command
returned zero, its closing-pin checks completed, and its supervisor confirmed
owned process-group 60434 absence and reaped its retained leader. The whole
supervised attempt took 507.854436499998 seconds. Its external operator work bound
was 600 seconds, with five-second termination grace and 1 MiB stream limits;
the public producer's per-call bound remained 60 seconds. These are functional
observations, not ingest latency or a performance benchmark.

The exact 910-byte prefix receipt has SHA256
`2de33480b0ba4d65d1be7997eeeccbc06663846a86a954932e3e14e6d2d33fec`
and equals CLI stdout byte-for-byte. It records completed ordinals 0–2 (A1, B1,
A2), three publications out of the unchanged eight-slot schedule, source progress
A2/B1 and `run_complete: false`. The original run digest remains
`6e466770c8973b3c3104b48924c2e3e4c954745e63c054e0a14817299bc674ed`.
This is a closed three-publication prefix, not completion of the full run or
qualification of the five remaining ordinals. No lineage, journal, source or
consumer was reset. Earlier failed attempts and their cleanup failures remain
[separate](../current-combined-source-a2-retained-20261017/README.md).

Product source was `9a33d23b07f87eaf2d9e5792d8e0bad8786eb06c`, wheel SHA256
`0d7002810555cd56ea2d02d829e068bc35873bdb8edbcfa5fc5f849ad504879e`.
The diagnostic helper was separately pinned to
`35d1849b02dc7abaa42801b64ba0d318d306ba00`. Preparation's pre-run pending state
and actual root launch clearance are retained separately. No deployment or
authority is inferred from those selections. The existing finite local
Spark/Delta runtime, original public producer, native targets and ordinary
source/ACK registrations were reused; the runtime/source closures remain pinned
by preparation and prior evidence rather than copied again.

A subsequent separate ordinary-role source/protected-ACK readback passed in
0.8911322921 seconds, with transaction closing and supervisor cleanup confirmed
for process group 71177. Both source heads were four; all four original source
groups matched opening and closing. A's protected position was two, matching
completed slot 2; B's was one, matching completed slot 1. Exact original request
and manifest bytes matched, and the public protected-receipt codec was checked.
The 129,865-byte report has SHA256
`01f79a325b9ace9aaa223a0367d430d277b456dd39eb9af7cc3ac709623bdc38`.
This readback rolled back and closed its ordinary sessions and submitted no ACK.
Its pending/operative preparation and root review are retained. Protected local
ACK service checks do not establish production authentication or remote fencing.

The original A2 request and selected completed manifest are retained as bounded
exact bytes alongside the read-only ledger selection. They are correspondence
evidence, not a new authority grant. The fixed-label trace records successful
diagnostic closing; its nested inclusive durations are not additive or isolated
SQL timings. Raw private stack and credential-bearing operator environments are
excluded. The earlier canonical ordinal-0 verification selected before A2 is not
repeated or promoted into a historical-read-after-A2 claim here.

`actual.tar.gz` retains 37 exact records in 45,456 bytes: selected controls,
receipt, terminal, runner and trace, and the separate actual source/ACK readback
and its exact expected bytes. `manifest.json` binds each original length/hash,
the deterministic gzip bundle, and hash-only prior/binary/private references.
No SDK/source closure, wheel, Delta/SQLite payload or secret environment is
duplicated. This archive establishes no full-eight result, cross-engine refresh,
Databricks/Fabric execution, production security or general throughput claim.
The assembler read retained files only; it made no engine, PostgreSQL, source,
ACK, publication, replay or retry call.
