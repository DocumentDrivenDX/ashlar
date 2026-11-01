---
ddx:
  id: ashlar.evidence.truss051-fresh-source-packet-20261031
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.truss051-r21-local-restore-candidate-20261031
      kind: informed_by
    - id: ashlar.evidence.truss051-r20-runtime-cohort-20261031
      kind: informed_by
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
---

# Fresh Truss 0.0.48 source capability packet

**Status: draft; r6 successfully accepted a fresh local development model,
then stopped before consumer enrollment.** The accepted registration is
revision 1, registration `bc6e5004-b07f-4a25-9c35-19a08c4b5f39`, for document
`ashlar-local-dev-typed-graph-r6-20261031` with SHA-256
`2c9b295a8232cae4770255650bf634941ab223e582b79ae2bb78f6acf0b8a880`. This is
local-development authority only. Truss requires an ordinary LOGIN session
principal with registrar membership and `role=none`. R4 used `SET ROLE` under
the PostgreSQL superuser, while the generated registrar itself is NOLOGIN. R5
used `NOINHERIT` and could not see the registrar's EXECUTE grant. R6 used
`INHERIT TRUE, SET FALSE`, passed the public read-only registrar status check,
and accepted the model through Truss's public API. R4's receipt is at
[`r4-local-attempt.json`](r4-local-attempt.json); r5's is at
[`r5-local-attempt.json`](r5-local-attempt.json).

The complete r6 outcome is in [`r6-local-attempt.json`](r6-local-attempt.json).
Its raw failure receipt has one stale mutation flag, corrected in
[`r6-receipt-correction.json`](r6-receipt-correction.json). Consumer enrollment
stopped before mutation because the scope JSON contains a duplicate
`ordinary_reader_role` key; the later 64-byte prose value replaced the intended
35-byte role name and failed Truss's public bound check. No feed consumer, seed,
or feed read was created.

## Exact package and API evidence

The retained wheel is
`/private/tmp/ashlar-r16-truss048-readonly-wheelhouse/truss_toolkit-0.0.48-py3-none-any.whl`,
SHA-256 `1aedf614ff3d3ff7a61bd4d91887ede650a7135d1239e8564c1c297c0ddc37c3`.
The inspected environment is
`/private/tmp/ashlar-r16-truss048-readonly-compat-312/bin/python`, Python
3.12.14, with `truss-toolkit==0.0.48` and psycopg 3.3.6. The separate fresh
trial environment verified the local runtime pins `pgserver-pg18==18.6.1`,
`fasteners==0.20`, `platformdirs==4.12.4`, and `psutil==7.2.2`.

Public symbols and implementation locations in the pinned wheel:

- `truss.LocalPostgres(data_directory)` in `truss/local_runtime.py` owns an
  isolated local PostgreSQL 18 cluster. Its context initializes an empty
  directory with the bundled `initdb`, verifies server version 18.6, starts
  the server, then stops it on context exit while retaining data. This is a
  native effect and was not invoked here.
- `CatalogDocument(document_id, document_revision, content,
  artifact_identity=None)`, `prepare_acceptance(documents, configuration=None,
  bun_executable='bun')`, and
  `CatalogReferenceAssembly.accept_in_transaction(transaction, prepared,
  request_key=..., origin_bytes=..., expected_revision=0,
  expected_documents=())` provide public catalog acceptance. Acceptance returns
  a `CatalogRegistration` with a generated registration ID and revision.
  `truss/cli.py` also contains a fresh-install example using
  `install_in_transaction`; it was not called.
- `register_feed_consumer_in_transaction(transaction, consumer_id=...,
  reader_role=...)` is an installation-administrator operation. The pinned
  implementation enrolls a full-source consumer from genesis, creates its
  registration ID and initial `runtime_feed_cursor(..., '0')` checkpoint, and
  grants feed functions to an ordinary reader role.
- `apply_in_transaction(GroupSemanticInput, GroupRequestSelection, documents=...)`
  applies one atomic Truss group with caller-owned commit. The alternative
  `import_in_transaction(ImportInput | OwnedImportInput, documents=...)` is
  also public. `read_feed_in_transaction(transaction, consumer_id=...)` reads
  the next whole committed transaction. None were invoked.

`check_public_api.py` verifies the installed version and wheel digest, byte
correspondence of the six imported API modules with the pinned wheel, public
method presence/signatures, and local-runtime contract text. Its result
is recorded in `api-check-result.json`. This confirms API availability and
shape only; it does not prove runtime behavior.

## Source authority and inputs required

The APIs can produce a real Truss acceptance and mutation in a local development
cluster. They cannot turn operator-authored input into an authoritative business
source. This packet is expressly labeled `local_development_authority`; it must
not be represented as production source, production acceptance, or source-owner
custody. The stale r3 intake in `source-authority-inputs.json` does not govern the
r6 local-development continuation or supply its source bytes.

The historical A source of record was
`/private/tmp/ashlar-truss045-real-source-20261021/model/source.umf.json`
with SHA-256 `498a82f313fd3ceaeb6195e133b403aa829acfcda7cec676a33a5790a57d456b`,
but that file and its acceptance-origin file are no longer present at their
retained paths. The repository's `truss-current-catalog-0.10-20261021/accepted-source.umf.json`
has a different SHA-256,
`19ab3cc44efd769346b88fb7c5dc6792b59fd0057ed8846e4a0eec744f8e716b`,
and the [source-byte review](../../current-upstream-release-20261022/r04-b-source-installation-draft-20261023/source-byte-reuse-candidate-20261023.md)
explicitly excludes it as a substitute. A new authoritative source handoff is
therefore required for a claim tied to that historical A source. A separately
declared local development source may use new bytes with its own identity and
scope, but cannot inherit A's acceptance or origin.

The prior seven-operation seed request is retained at
`docs/helix/04-build/evidence/current-upstream-release-20261022/truss-049-lifecycle-ready/seed-request.json`
(SHA-256 `56aed4d7a4789f45cdaa7af55a834ff38ca2b4bcb75f44658b3031ba5f8aa771`).
R20 proposed an explicit `local_truss_acceptance_trial` origin meaning for these
operations, separate from historical acceptance-origin evidence and production
authority. The R20 attempt approval was for its exact scope and is consumed;
although it failed before seed execution, that approval does not transfer to a
new cluster, source identity, catalog registration, consumer, seed execution, or
`.48`→`.51` migration. That retained historical seed request is not reused. The new seven-operation
local seed has independent bytes and identity, and is covered by the standing
local-development authorization bound to the r6 migration scope.

R21 rejected the retained R16/R20 PostgreSQL data copy because 13 required
PostgreSQL 18 directories are missing, including paths whose contents may be
non-empty; the copy is not an input to this fresh-source proposal. The clean
fresh-cluster route avoids relying on those bytes, but it does not resolve source
authority or qualify the `.48`→`.51` migration. The follow-on packet now
proposes migrating a clone of the accepted r6 `.48` source before creating a
consumer, checkpoint, or seed under `.51`.

No PostgreSQL process, source mutation, feed read, migration, Spark/Delta,
publication, ACK, or cloud action was performed while preparing this packet.

## Fresh local-development trial candidate

The owner selected a fresh source through Truss public APIs. The prepared
candidate uses the frozen G03 typed-graph model as its starting point, with one
exact byte substitution to give the model and Truss catalog document the same
new local-development identity. The remaining model bytes and declared UMF
0.10.0 semantics are preserved. The source document ID and revision, catalog
request, source-system/epoch labels, consumer and ordinary reader role are all
new. The seven-operation seed is newly authored with local values; no historical
A seed request bytes or acceptance are reused. See
[`fresh-local-trial-scope.json`](fresh-local-trial-scope.json),
[`fresh-local-seed-request.json`](fresh-local-seed-request.json), and the
[effect-free preparation result](effect-free-preparation-failure.json).

The first candidate stopped before PostgreSQL startup because Truss rejected a
document ID that did not match the UMF `id`. The derived model changes exactly
that one JSON token; it matches the new document ID and passed Truss's public
pure preparation call. The second local attempt stopped at `initdb` because the
restricted sandbox denied POSIX shared memory. The authorized escalated r3
attempt started PostgreSQL 18.6, then stopped before Truss installation when
`host.adopt_transaction()` returned an error. A focused read-only preflight
against that retained cluster passed after setting a 5-second transaction
timeout; see [`r3-host-transaction-preflight.json`](r3-host-transaction-preflight.json).
The single r4 attempt used the corrected timeout and successfully installed the
Truss runtime. Public catalog acceptance then returned `P1403` with
`catalog_code="unauthorized"`; see [`r4-local-attempt.json`](r4-local-attempt.json).
The exact failure is retained in the attempt tree. Read-only inspection found
that the generated registrar has `rolcanlogin=false`; Truss's `runtime_person`
guard requires `rolcanlogin=true`, a safe actor, no active `SET ROLE`, and
registrar membership. The public status API returned unauthorized for the
NOLOGIN registrar. R5 then confirmed that `NOINHERIT` blocked the original
EXECUTE grant (`42501`, `original invocation EXECUTE grant required`) in its
read-only status preflight; see [`r5-local-attempt.json`](r5-local-attempt.json).
R6's `INHERIT TRUE, SET FALSE` membership passed public status and acceptance.
The consumer call exposed the duplicate scope key described above. Its initial
`.48`-only continuation design remains as superseded review evidence in
[`r6-continuation-inputs.json`](r6-continuation-inputs.json),
[`check_r6_continuation.py`](check_r6_continuation.py), and
[`r6-continuation-validation.json`](r6-continuation-validation.json). It is
superseded by the `.51` migration packet below. All attempt directories remain
under `/private/tmp`; none has been cleaned up.

Truss installation and public catalog acceptance succeeded locally, but feed
consumer enrollment, seed submission, and feed read did not. This is not
`.48`→`.51` migration evidence. The Sol-reviewed one-shot packet migrates a **clone** of
accepted r6 to `.51` before consumer enrollment or seed, preserving the original
`.48` PGDATA unchanged. It gates on complete PG18 layout and exact source-tree
hashes, creates separate cold rollback and migration copies, follows the pinned
route `.48` `c4339ac7…` → `.49` `35fd797b…` → `.51` `288801f7…`, and requires
public migration receipt plus accepted-document, registration, report and
complete-view readback before later mutations. It then configures safe roles
and module access, enrolls a genesis consumer, applies one seven-operation seed,
and reads one feed page without ACK. Exact paths, role strings, API arguments,
and limits are in [`r6-truss051-migration-scope.json`](r6-truss051-migration-scope.json).
The effect-free result is recorded in
[`r6-truss051-effectfree-validation.json`](r6-truss051-effectfree-validation.json),
with its checker at [`check_r6_truss051_packet.py`](check_r6_truss051_packet.py).
The
review-gated worker and resource supervisor are
[`r6_truss051_local_worker.py`](r6_truss051_local_worker.py) and
[`r6_truss051_supervisor.py`](r6_truss051_supervisor.py). The OS process
inventory is required immediately before dispatch; this sandbox cannot run
`/bin/ps`, so no ownership conclusion is recorded here. The one-shot candidate is bound to the user’s standing local authorization in
`r6-truss051-native-approval.json`; the authorization does not extend to cloud
operations, production authority, publication, ACK, or cleanup. No `.51` migration
or continuation action has yet been performed on r6.


## Dispatch candidate status

Sol reviewed and accepted the bounded local `.48`→`.51` clone, consumer, seven-operation seed, and first-page read scope, subject to this exact authorization binding. The user’s standing authorization covers fresh Truss source creation, a local seed, and a tested release; no additional user approval is requested for this one-shot local-development attempt. The scope uses `approved_one_shot_native` / `one_run_local_native_lifecycle` and is bound by [`r6-truss051-native-approval.json`](r6-truss051-native-approval.json) to the exact scope, worker, and supervisor hashes. The earlier r3 source-authority intake is stale and is not authority or an input for this continuation. The retained r6 `.48` PGDATA remains the sole source; it is copied only to bounded cold-backup and migration-clone destinations, and is not modified. Cloud, production authority, publication, ACK, and cleanup remain excluded. This packet update does not itself dispatch native work.


## R7 handshake correction candidate

The reviewed R6 supervisor invocation failed before worker output or PostgreSQL effects: it created the supervisor directory and child logs, then the worker incorrectly required that directory to be absent. R6 is consumed; its source PGDATA and prior receipts are retained. R7 uses distinct `/private/tmp/ashlar-r7-truss051-continuation-20261031` output paths and migration attempt ID. Its worker now requires the output root to remain absent while verifying that the supervisor-created root is canonical, user-owned, private, and contains exactly the two expected owned child logs. [`test_r7_worker_handshake.py`](test_r7_worker_handshake.py) exercises that exact pre-child state plus output-root and missing-log refusals, and the effect-free checker runs all three cases. Sol accepted the R7 candidate. The scope is dispatchable under its exact one-shot status and the user’s standing local authorization is hash-bound in [`r7-truss051-native-approval.json`](r7-truss051-native-approval.json); no new user approval is requested. The effect-free result is recorded in [`r7-truss051-effectfree-validation.json`](r7-truss051-effectfree-validation.json). No R7 native dispatch has occurred.
