---
ddx:
  id: ashlar.evidence.truss052-r34-custody-reconciliation-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-002
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
---

# R34 Truss source-custody reconciliation

R34 committed one local-development genesis publication and completed the
independent Delta v1 readback, then stopped before acquiring a Truss source
custody handle. The selected first feed page is a zero-entity catalog/revision
prerequisite: page SHA-256
`942a325702683382c99bd92c69a1af6478cf04879fee0f9d8db4cd4e80f8a9a0`, prior
cursor SHA-256 `9afcb17ae6f64d065c0bc11f25c2f111c43b95ade0bd20a354a568001a91ad4a`,
next cursor SHA-256
`8e370e1497197b93029ff7f5287d9102802a098fbc4ea02b1e6bf20339841f9f`.

The exact failure is PostgreSQL SQLSTATE `42501`, `permission denied for
function catalog_status`. The worker called Truss `.52`
`CatalogReferenceAssembly.status_in_transaction` as
`ashlar_local_dev_reader_r21_20261101` inside `Truss052SourceCustodySession.open()`.
The PG log records the failed statement as `SELECT truss.catalog_status()`;
the assembly returned an `Error`, which the Ashlar adapter projected to its
generic `Public Truss source-custody operation refused` exception.

A read-only `pg_catalog` ACL census against the stopped R34 clone confirmed the
role split:

| Principal | `catalog_status()` EXECUTE | `open_feed_source_custody()` EXECUTE | Membership |
| --- | --- | --- | --- |
| `ashlar_local_dev_reader_r21_20261101` | false | true | module reader true; registrar false |
| `ashlar_local_dev_registrar_r21_20261101` | true | false | registrar true; module reader false |

This matches Truss `.52`'s public setup: installation grants catalog status to
its registrar/client role, while `register_feed_consumer_in_transaction`
grants feed and custody entrypoints to the enrolled reader role. The bounded
repair is to obtain a fresh public `CatalogRuntimeStatus` through the registrar
immediately before custody open, pass that status to the Ashlar adapter, and
open/renew/ACK/reconcile/close on the original feed-reader connection. No
Truss grants or role memberships need to change. The database-side
`open_feed_source_custody` operation remains the final source fence.

R34 publication reconciliation is positive and ACK reconciliation is absent:

- `receipts/genesis-native-readback.json` binds publication
  `truss-graph-cbf69ea847e30930ec586b2a3d508745f0430da3e9e1628f6b81149b9d58230b`
  to the exact manifest/vector/receipt and four local data-role Delta v1 heads.
  Each data role has zero rows and the empty-row-bag SHA-256. The independent
  log heads match: edge `711854f9…`, object `9fe73e90…`, tombstone `b967a5a7…`,
  history `183fbb2d…`.
- `delta/result.json` records one publication, six committed SQLite operations,
  and `source_acknowledgement: deferred_not_attempted`. The SQLite journal has
  one publication artifact and no ACK operation.
- `receipts/durable-deferred-ack-intent.json` points to the exact retained
  sidecar, SHA-256 `4c47bf067c628ed57c10d8ebabe2529af0001068b813b31f813e38c64790f82f`.
- `receipts/failure.json` says `source_ack_attempted: false`; there is no ACK
  reconciliation receipt. The public-source reconciliation retains the
  original prior cursor, so the prerequisite page remains unacknowledged.
- The supervisor records no worker, Spark, or PostgreSQL process remaining and
  reports cleanup complete. No cloud effect occurred.

The read-only SQL check briefly restarted and shut down only the retained R34
PGDATA clone to inspect installed role grants. It appended its own PostgreSQL
startup/query/shutdown lines to `pgdata/log`; it did not change Truss rows,
Delta tables, the SQLite journal, or R34 receipts. The post-diagnostic PGDATA
tree digest is
`040605a319179ea343416f886b45a7852abba8d47b0e7308daac1d241492a9f5`
(1,259 files, 64,936,290 bytes). The R34 Delta tables and receipts remain the
publication evidence and are retained at `/private/tmp/ashlar-r34-truss052-page-sequence-20261101`.

R34 is in a no-ACK, committed-destination state suitable for a recovery
design. A future local recovery must revalidate the public page and exact
committed destination, then open the same-session custody handle and ACK only
the retained receipt. It must not replay acceptance, consumer enrollment, the
genesis group, or the committed Delta publication. A later continuation can
proceed only after that exact ACK has matched and the source checkpoint equals
the retained next cursor. The R35 packet remains pending independent review;
its read-only rehydration of the strict native observation and transport
identity must be solved before dispatch. No R35 native effect has run.

Retained receipt digests:

| File | SHA-256 |
| --- | --- |
| R34 failure | `907e2703bf3562d4ab08eeb2e9cf6185e8c3782921460fab30f08a0c7cfde2c1` |
| R34 native readback | `c8efec9c204ba43960a4e345c0d4e6cef58dfd1e3d1e61941f7e623355a9b8c2` |
| R34 deferred-ACK receipt | `8f0f6d59b49df00f879711162fff120a752e90dd818cdb592baa055734951f06` |
| R34 Delta result | `68d9d40000b19ff3bdab03f022bc5eb4d9c1612bcf54f92e45278a9e87b56c1c` |
| R34 operations SQLite | `7dd748ef5b023144719d307cb87d641a583bde9d94f68590b1e55dc08d8485f6` |
| R34 supervisor receipt | `c1b7338692e04983c52096f1cac8a7b464e6ce3fdf61f6433e579925fc5b82aa` |
