---
ddx:
  id: ashlar.evidence.source-holder-truss051-capability-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: informed_by
    - id: CONTRACT-003
      kind: informed_by
    - id: TD-009
      kind: informed_by
    - id: ashlar.evidence.source-holder-interval-proposal-20261023
      kind: informed_by
    - id: ashlar.evidence.truss051-r16-effectfree-candidate-20261029
      kind: informed_by
---

# Truss 0.0.51 source-holder capability inspection — 2026-10-29

**Disposition: source-holder integration remains unavailable.** The contract opens the external
holder only after durable verifier success, for the bounded protected ACK,
reconciliation and close interval. Truss 0.0.51 exposes current page read, retained-page reread, exact-receipt ACK
and ACK reconciliation. It has no named source lease or renewal handle. A
read-only `.51` probe now checks current and retained page identity; a bounded
effectful holder remains unavailable until live managed-read and native-pin
owner handles exist and the orchestration is reviewed. No source, publication, ACK, or native method was
invoked.

## Selected package and public API

The exact wheel at `/private/tmp/umf_truss-0.0.51-py3-none-any.whl` is 6,240,193
bytes with SHA-256
`eb59141c48e94bb93ab7053e9e8725012eee1749e4432e9c71804d23abbdff57`, matching
the selected R16 package pin. The isolated R16 Python 3.12.14 environment
reports `umf-truss==0.0.51`.

The installed public `NativeFeedCapability` protocol accepts a transaction for
`read_feed_in_transaction` and `reread_feed_in_transaction`. It also exposes
`acknowledge_feed_with_receipt_in_transaction` and
`reconcile_feed_ack_in_transaction`, with destination ID, request digest,
publication ID and exact receipt bytes. In the selected 0.0.51 runtime the exact
signatures are: `read_feed_in_transaction(self, transaction, *, consumer_id,
maximum_facts=4096, maximum_bytes=8388608)`;
`reread_feed_in_transaction(self, transaction, page, *, maximum_facts=4096,
maximum_bytes=8388608)`; and both ACK/reconcile methods take
`(self, transaction, page, *, destination_id, request_sha256, publication_id,
receipt_bytes)`. The return types are `Outcome[FeedPage | FeedUnavailable]`,
`Outcome[FeedPage | FeedUnavailable]`,
`Outcome[FeedReceiptAcknowledged | FeedUnavailable]`, and
`Outcome[FeedAckReconciled | FeedUnavailable]`, respectively. The public `FeedPage` retains exact
cursor bytes and page content/digest; `FeedAckReceipt` retains installation,
epoch, consumer and registration IDs, both cursors, feed digest, destination,
request digest, publication ID, and exact receipt bytes/digest. These types
support page and receipt correspondence. The public protocol has no named
source-custody open/renew/close or pin-retention operation, lease lifetime, or
source-owned interval object. The contract does not require the source holder
to fence writers during verification: it begins after the verifier has
durably succeeded.

Inspected public definitions: the pinned venv's
`truss/capability_contracts.py:103-115`, `truss/feed.py:11-77`,
`truss/execution.py:15-37`, `truss/catalog_runtime.py:1566-1660`, and
`truss/_catalog_runtime/feed.sql:268-341`, `feed-receipts.sql:41-95`. The package
version and wheel digest were checked read-only; package import performed no
Truss method call.

## Long-held transaction assessment

Truss issues a caller-lifetime `TransactionHandle`; its host does not begin,
commit, roll back or close the caller transaction (`execution.py:2-6,15-21`).
The feed API accepts that handle, so Ashlar can keep one connection and
transaction open during the bounded post-verifier ACK interval. This is a
possible Ashlar-owned live handle, not a Truss-issued lease. The contract does
not require that connection to span verification.

The source SQL limits what a held transaction proves. `read_feed` explicitly
does not acquire row locks during polling and checks the captured revision and
checkpoint only around that call (`feed.sql:268-284`). `reread_feed` rechecks
authorization, retained page bytes and revision around that call, but does not
lock or prove the current consumer checkpoint (`feed.sql:315-340`). Therefore
repeated `reread_feed` alone can still reproduce an old page after ACK; it is
not a current-checkpoint renewal. A long-idle transaction also does not reserve
the feed consumer row against another ACK.

A bounded post-verifier Ashlar owner could use the public API without a new
Truss method only if each renewal independently calls both `read_feed`
(requiring the same consumer registration, original `prior` checkpoint and
exact current page) and `reread_feed` (requiring the original retained page's
exact bytes and cursors). Any source, cursor, page, authority or deadline drift
must stop the attempt. Immediately before ACK, the owner must repeat those
checks. Truss's receipt ACK then locks the consumer row
`FOR UPDATE NOWAIT`, revalidates the exact delivered interval, advances the
checkpoint and retains the opaque receipt atomically in the caller transaction
(`feed.sql:287-309`, `feed-receipts.sql:41-70`). After commit, exact
reconciliation requires a fresh read-only, unassigned transaction and the
owner must retain the exact receipt match plus independent closing checks
(`feed-receipts.sql:74-95`). The ACK compare-and-set refuses an intervening
checkpoint advance; the earlier read calls are active checks, not a lease or
writer exclusion during verification.

This defines a candidate source-owner adapter boundary, but does not establish
an adapter or interval contract. It need not span the verifier. Actual `.51`
source/registration/cursor admission, bounded deadline, operation
interleaving, ACK commit, fresh reconciliation and closing behavior need a
controlled native source run and independent review. R16 does not admit such a
run, and this inspection did not perform one.

## Ashlar call-site boundary

Ashlar's ordinary `TrussFeedSessions._read` calls the public page-read method
inside `_assembly`, then closes the connection in its caller
(`src/ashlar_host/truss_feed_sessions.py:38-49,156-168`). The added
`renew_051_source_custody` calls public current-page read followed by exact
retained-page reread in one read-only transaction
(`src/ashlar_host/truss_feed_sessions.py:233-242,388-402`). Gated
`reconcile_051_source_interval` and `close_051_source_interval` perform fresh
read-only receipt reconciliation and independent source close
(`src/ashlar_host/truss_feed_sessions.py:249-263,404-430`). The public
`acknowledge_post_verifier_source_interval` fails closed; there is no supported
effectful composition because no live managed-read/native-pin owner inputs exist.

The versioned `.51` adapter only verifies the installed distribution/profile
and delegates one read-only R04 page read
(`src/ashlar_host/r04_truss051_feed.py:10-28`). The managed current Spark reader
is an offline verifier, explicitly exposes no ACK port, and its selected source
versions end at `.50` (`src/ashlar_host/managed_current_spark_receipt_read.py:78-125`).
The method signatures above were inspected using
`/private/tmp/ashlar-r16-truss051-effectfree-312/bin/python` and
`inspect.signature`; this inspection imported type definitions only and invoked
no Truss method. R16 remains effect-free preflight: its execution receipt says
`native_effects=false` and `execution_admitted=false`; expected accepted source,
feed, consumer, and checkpoint custody was not observed. It cannot provide a
real `.51` source selection for a local holder run.

## Exact next integration seam

Ashlar now retains only read-only `.51` current-page plus exact retained-page
renewal (`TrussFeedSessions.renew_051_source_custody`) and separately gated
reconciliation/closing primitives. These call public `read_feed_in_transaction`,
`reread_feed_in_transaction`, and (for reconciliation only)
`reconcile_feed_ack_in_transaction`; they do not ACK. The exported
`acknowledge_post_verifier_source_interval` boundary fails closed unconditionally
until live managed-read and native-pin owner handles are available. In
particular, `ManagedCurrentSparkReceiptReopenObservation` is constructible
offline output, explicitly has no ACK port, and is rejected without touching
source methods. A protocol-fake regression proves no ACK, reconciliation, or
close call occurs through that rejected route.

The future source-only sequence, after valid live owner inputs exist, is: verify
current `.51` page and exact retained page immediately before ACK; invoke exact
receipt ACK CAS with the original request/manifest bytes and full vector; commit
once; reconcile from a fresh read-only transaction; then independently close by
checking the advanced cursor and retained original page. Any uncertain outcome
uses reconciliation only; it never retries the ACK. This describes the intended
seam, not implemented behavior. Neither protocol fakes nor this source-only
read path are a complete managed campaign holder or release gate.

## Minimum owner capability before Ashlar integration

Before adding a Truss-backed `TrussSourceCustodyPort`, Ashlar must independently
select and retain the original registration/cursor bytes and bind a bounded
live owner to the installation, source/feed registration, epoch, consumer,
exact checkpoint/page, destination registration and ACK scope. Current evidence
does not establish that a new upstream lease API is necessary; require one
only if controlled source testing shows repeated exact reads plus ACK-CAS cannot
provide the contract's bounded active checks and closing.
Exact receipt ACK and fresh reconciliation must preserve the original
request/manifest/receipt bytes and full publication vector. Expiry, lost
authority, drift or uncertain outcomes must refuse completion and preserve
original recovery custody. Whichever boundary supplies renewal, the external
source principal, actual native transaction behavior and close semantics need
review before Ashlar claims the contract is satisfied.

## Release implication and check scope

Release-closure gates 2 and 4 remain blocked on the source holder; the existing
proposal helper remains fake-port choreography only. The next executable slice
requires actual accepted `.51` source/feed/consumer/cursor custody, live admitted
managed-read and native-pin handles, and a reviewed bounded source-owner
strategy using public operations or a versioned upstream custody API if needed. It must not be represented by a local transaction alone, caller
assertion, synthetic feed, or adapter that merely checks method names.

Verification includes read-only wheel hash/size, installed version and exact
public API signature/dataclass/SQL inspection; call-site inspection; four
focused source-seam protocol-fake tests; and 16 combined focused source receipt
and session tests. No real Truss method, native source, or cloud operation was
invoked. There was no effectful test.
