# Fresh combined-source prefix: partial execution

A separately provisioned local A/B source and canonical Delta installation were
used for one actual `publish-commerce-evolution` attempt. The original failed
lineage was preserved. The new source fixture uses its own container, ordinary
roles, consumer and ACK scopes; its seed retained eight original, publicly
admitted batches. The empty six-table `native-r2` bootstrap completed its strict
UUID/location/schema/nullability/protocol checks and required closing before the
publisher started. These preparations do not themselves grant source/ACK or
publication authority.

The installed Ashlar wheel came from
`cf3e9028a0252b8504475763d1d757238e626a2e`, SHA256
`c1f448fb451426e954fc788876a737c99324a108de1ce14c66c0c74f2719b2f3`.
Its configuration retains the complete eight-slot A/B schedule, with a requested
prefix through ordinal 2. The real provider selects the existing source, ACK and
native owners. This is a fresh lineage, not continuation or restoration of the
older missing-table installation.

The attempt reached its 180-second work deadline without a final receipt or
successful stdout. The retained terminal reports `TimeoutError`,
`cleanup_confirmed: false`, `cleanup_failed: true`, and `complete: false`.
After a five-second graceful cancellation window, cleanup killed the owned groups
before reaping their leader. Its later absence check failed with `PermissionError`
after the leader was reaped. The separate post-terminal elevated read-only process
observation found no rows for the two recorded groups. That later observation does
not repair the original cleanup result or authorize signals against reaped IDs.

The retained read-only ledger observation shows ordinal 0 completed with its
original descriptor, ordinal 1 retained without a completed descriptor, and
ordinals 2–7 unprepared. This supports a partial execution record, not successful
completion of the requested three-publication prefix. Ledger state alone supplies
no protected ACK or complete native-content verification; the observation labels
both limitations explicitly. No final prefix receipt was released.

A separate ordinary-role source/protected-ACK observer subsequently completed
successfully, including its own closing and supervisor cleanup. Both source heads
were `4`, with complete original four-group inventories and equal opening/closing
observations. Protected readback showed A at position `1` and B at `0`. A's exact
stored request and manifest bytes match the separately pinned retained completed
slot-0 selection; its protected receipt bytes are retained. B has no protected
receipt, and its `selected_slot0_bytes_match: false` is expected because slot 0
belongs to A. Scope bindings and ordinary privilege checks remain mandatory.
These observations used rollback/close, submitted no ACK and did not perform
native publication verification. They establish observed durable A1 progress,
not completion through ordinal 2 or repair of the original cleanup failure.

A single diagnostic sample scheduled at 120 seconds contains Ashlar source
checkpoint/request correspondence and evolution-ledger admission inside publication
writer renewal. Only bounded module/function/line labels and the private sample's
hash are retained. This is not a timing profile, proof of a native bottleneck, or
an indication that those checks may be bypassed.

The original configuration, provider, supervisor, immutable run reservation,
bounded stderr, bootstrap/seed/admission preparation results, and separate ledger
and process observations, later source/ACK proof scripts and exact protected
readback are retained as exact streams. No raw operator
environment, DSNs, private stack, credentials, binaries or bulk Delta/SQLite
payload is included. The actual local installation remains separate.

`manifest.json` binds original and compressed SHA256 digests and lengths; gzip
timestamps are zero. This is a finite local failure/partial-progress record, not
throughput, latency, full recovery, cloud or production-authority qualification.
The archive assembler made no live calls or retries; it only copied retained
artifacts, including the separate later ordinary read-only source/ACK observation.
