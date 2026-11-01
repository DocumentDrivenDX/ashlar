---
ddx:
  id: ashlar.evidence.r19-truss051-catalog-authority-follow-up-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: informed_by
    - id: CONTRACT-002
      kind: informed_by
    - id: TD-004
      kind: informed_by
---

# R19 catalog-authority follow-up

R19 verified the accepted R6 document and first page through public Truss `.51` APIs, then failed before publication. The page receipt matches the R16b page, including source registration `fdc09c3f-be99-444a-8031-a48ae4f527ec`, prior checkpoint `802f677f84076a2deaa1da682c836550a38654e2e1b67494b9cd2acf01299725`, page digest `de5c6d50f03082e5650126db86b4b50871a70f3a74f183a06a1f645a31e51760`, and next checkpoint `195f826d5257fbcd920c19569d9dc33930dfeef22aa7ee14dce3e6c3fe4f280c`. The accepted document is `ashlar-local-dev-typed-graph-r6-20261031`, revision `1`, registration `bc6e5004-b07f-4a25-9c35-19a08c4b5f39`, catalog revision `1`, content SHA-256 `2c9b295a8232cae4770255650bf634941ab223e582b79ae2bb78f6acf0b8a880`.

The exact public catalog-view outcome captured from the retained R19 clone is Truss `Error` with SQLSTATE `P1403`, catalog code `unauthorized`, principal `ashlar_local_dev_reader_r6_20261031`, and transaction mode `read_write`. The `.51` `_catalog_runtime/person-entrypoints.json` declares `person_catalog_view_basis` as registrar mode. `require_person_invocation` admits registrar mode through the installation registrar membership; the existing feed reader is deliberately not granted that authority. Truss also declares `person_accepted_document` as catalog mode, which accepts a module reader/writer or registrar, subject to the original routine EXECUTE grant and current module-read authorization.

The narrow continuation design keeps page reads under the dedicated feed-reader connection and passes a separate explicit catalog connection factory for current report/source/view evidence. R19 did not run a registrar accepted-document ACL preflight, so the exact grants for this second principal remain unverified. R20 must use the existing local registrar principal and prove its original and person accepted-document EXECUTE privileges plus module-writer membership with a read-only `pg_catalog` query before Spark. It must then read exact accepted-document bytes and the complete view on that registrar connection in one bounded READ COMMITTED transaction, matching report revision, registration, source document identities and digests to the page's existing catalog prerequisite. Truss `.51` requires catalog view to use a PostgreSQL READ WRITE transaction (`view.sql` returns NULL under `transaction_read_only='on'`); its basis function uses SELECT and `FOR SHARE NOWAIT` only and the caller rolls back. This is described as effect-free, not as PostgreSQL read-only.

This follows [CONTRACT-001](../../../../../02-design/contracts/CONTRACT-001-publication-boundary.md) native Truss continuation/source identity and publication-before-ACK constraints, [CONTRACT-002](../../../../../02-design/contracts/CONTRACT-002-consumer-read-boundary.md) source-scoped read authorization, and [TD-004](../../../../../02-design/technical-designs/TD-004-truss-catalog-preparation.md) public Truss source-boundary/effect-exclusion rules. It does not introduce a fixture, role bypass, source mutation, or ACK. The R19 source and seed remain a local development source only.

## Retained receipts

The detailed artifacts are retained under `/private/tmp/ashlar-r19-truss051-delta-publication-20261101` and its `-supervisor` companion. The immutable reconciliation in this directory records the per-table Delta log hashes and every zero-data-action assertion. Key artifact hashes from that reconciliation are:

| Artifact | SHA-256 |
| --- | --- |
| `receipts/failure.json` | `892da9333f3e6a83f23ca8f86c07beb498d0741e71ae74c6007fd3e7d7c63b1e` |
| `receipts/accepted-document-public-reread.json` | `4ff76ce7612f34736aa2377fc3a1d7a4fcb32634febc055c5f0bf3a3b9ec370a` |
| `receipts/original-unacknowledged-page-reread.json` | `6554b21170bb097da62162a2537583be6bb5a1f4d54fde9721d2b289eafc99b7` |
| `receipts/source-copy-custody.json` | `7a22632d87c38244a7810ad75cae627f9fe03e743f8baf043cd932f09b256cf3` |
| `receipts/spark-delta-runtime.json` | `981f85adc3b105259930a8e1faeffbf66e90ace257f3796b10d613e99459ff36` |
| `r19-child-stderr.log` | `8a71c4d2885b9274edb8f3f44b74f5d63d267d4b8a9b8d1ba5202b7c91f51fc5` |
| `r19-child-stdout.log` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |

R19's Delta archive is `/private/tmp/ashlar-r19-truss051-delta-publication-20261101/delta/source-page-1.json`, 44,329 bytes, SHA-256 `586955d9929dc826c447c21edcbc3b071b41e92bdf766bbcc825af33031143fd`. The failed clone tree is 65,758,216 bytes, 1,255 files, digest `a0f5209965e7e4fad527b15b88443d4866f5cb505af7d58397dcddf38a994698`; no PG owners remain and no postmaster PID exists. Its matching cold-backup/source digest is `b1ab6a55e3d13981e706b0cdb7ddd653ba7db9e241da6bef9ad4c16a47adc82e`.
