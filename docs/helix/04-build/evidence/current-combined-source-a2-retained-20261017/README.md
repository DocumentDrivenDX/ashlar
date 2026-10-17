# Combined-source A2 continuation: retained attempt

One actual `continue-commerce-evolution` invocation selected the existing
`native-r2` lineage and complete original eight-slot schedule, through ordinal 2.
Its original run SHA256 remained
`6e466770c8973b3c3104b48924c2e3e4c954745e63c054e0a14817299bc674ed`.
The provider and ordinary-source/ACK environment were unchanged from the
[earlier B1 continuation](../current-combined-source-b1-partial-20261017/README.md);
the invocation configuration changed only the selected ordinal and fresh receipt
path. The runtime selection is identified separately below. No lineage was reset
or re-created.

The installed runtime was the existing immutable Ashlar
`c370255b61bea622625f5123a4ecc87bb0408bb4`, wheel SHA256
`6d82e4c6f614025c3bbc15b65423f817c19fee3cf8a5d96cfbffe721bfe6805d`.
A2 preparation SHA256 is
`dfa0415464055484640eb2bcec68eed12ec32c7e2063de08c471cb018ab7cefc`.
Frozen preparation and later execution clearance remain distinct records;
readiness is not a completion claim.

The attempt reached its 180-second work deadline without a final prefix receipt
or successful stdout. The original supervisor reports `TimeoutError`, null child
status, `cleanup_confirmed: false`, `cleanup_failed: true` and `complete: false`.
Its post-reap absence check failed with `PermissionError` for recorded groups
`64708` and `67766`. A separate elevated read-only process observation later found
no rows for those two exact groups. That transcribed observation does not repair
the original cleanup result or authorize further signals after reaping.

The post-attempt read-only ledger observation shows slots 0/1 completed, slot 2
retained with 755,695 original bytes and no completed descriptor, and slots 3–7
unprepared. Its selected states/length are transcribed with the parent tool-output
reference, not presented as a SQLite snapshot or native-content verification.
This establishes a retained A2 attempt, not successful publication, protected ACK,
closed historical read or completion through ordinal 2. Partial native effects
are not resolved by these records. Prior A1/B1 protected readback predates this
attempt and supplies no new source/ACK inference after A2.

Exact controls, configuration, runtime provenance, bounded stdout/stderr and
original failure records are losslessly retained. Only the private diagnostic's
length and hash are included; raw stack, operator environment, DSNs, credentials,
binaries and bulk Delta/SQLite payload are excluded. No causal performance or
latency conclusion follows from this finite failure record.

`manifest.json` binds original/compressed lengths and SHA256 digests for 16
streams; gzip timestamps are zero. The assembler made no live native/database,
source, ACK or process observation, retry or tests. Any later helper probe or
continuation is outside this archive.
