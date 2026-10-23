# R05 canonical GraphFrames one-attempt packet — 2026-10-23

This separate packet targets exactly the independently reviewed R05 identity
discriminator: five `object_current` nodes, four `edge_current` edges, the
source-A `(1,101)` seed, and its three edge occurrences under cap 3. The exact
source, generator, independent oracle, canonical manifest, external anchor,
UMF carrier definition, action module, and materializer are individually
hash-bound in `invocation-packet.json`. The source inventory keeps two digests
separate: canonical source content `4fd7…f5f78` is supplied to the reviewed
descriptor; the retained `source-rows.json` file includes its terminal LF and
is pinned as `84f1…5057`. The anchor file is pinned at
`431085…26c6`, and its separately selected digest file is also retained.

The child worker is distinct from the existing r3 smoke worker. It admits the
reviewed bytes, calls `materialize_canonical_discriminator`, opens
`PinnedCanonicalGraphFramesActionFixture` with that descriptor and returned
paths, invokes `_verify_canonical_persisted_fixture`, and then makes one capped
GraphFrames one-hop call. The supervisor has R05-specific receipt checks for
5/4 persisted rows, cap 3, the source-A seed, and exact three-occurrence oracle
bag. The output location is bound as `attempt-root/fixture` beneath a fresh
mode-0700 attempt root. It uses the existing one-child process-group limit,
300-second total deadline, 2 GiB retained-plus-attempt monitored byte ceiling,
512 MiB attempt-root ceiling, and 128 MiB diagnostic ceiling. These are
periodically monitored filesystem limits, not kernel quotas.

## Execution remains fail-closed

No Spark or GraphFrames service was started. The packet is inert and cannot
admit an execution: its wrapper review is pending, execution authorization is
not issued, runtime executable/JAR paths are unset, and `cleanupStatus` is
`unqualified-no-owned-table-cleanup`.

The exact cleanup gap is that `materialize_canonical_discriminator` returns
only table paths and does not write an ownership receipt containing both Delta
UUIDs and path device/inode identities. The existing canonical cleanup helper
belongs to a different fixture source/profile and its receipt schema cannot
verify these UMF materializer outputs. There is no R05-specific routine that
reacquires `.ashlar-graphframes-action.lock`, proves both materialized paths
still have the attempt's exact ownership and Delta UUIDs, deletes only those
two paths, and records cleanup outcome. Therefore this packet does not authorize
the action. Implement and independently review that ownership/cleanup path,
then freeze new worker/wrapper/packet bytes and separately authorize one attempt.

The selected runtime tuple is Spark 4.2.0, Delta 4.4.1, GraphFrames 0.12.3,
JDK 21.0.13, and `local[2]`; the selected JAR SHA-256 values are recorded.
Exact interpreter, JDK, Spark distribution, and JAR file paths are unavailable
in the pending packet, so their file sizes also cannot yet enter retained-file
accounting. A future packet must bind the exact executable and JAR paths/hashes,
account for those files and distribution directories within the stated ceiling,
include a fresh slot check, and receive independent wrapper review plus a
separate one-attempt authorization. It must be immutably re-frozen; editing this
packet in place does not authorize execution.

No publication, source progress, protected ACK, Unity Catalog, production, or
scale qualification is claimed. This is an execution packet candidate, not a
native result.
