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

**Status: draft; effect-free API inspection only.** The pinned Truss 0.0.48
wheel contains public entrypoints for a fresh local PostgreSQL 18 runtime,
accepted catalog registration, genesis feed-consumer registration, and Truss
data mutation. The focused check imports and inspects those entrypoints only.
It does not start PostgreSQL, connect to a database, accept a document, register
a consumer, seed data, migrate, publish, or acknowledge a feed.

## Exact package and API evidence

The retained wheel is
`/private/tmp/ashlar-r16-truss048-readonly-wheelhouse/truss_toolkit-0.0.48-py3-none-any.whl`,
SHA-256 `1aedf614ff3d3ff7a61bd4d91887ede650a7135d1239e8564c1c297c0ddc37c3`.
The inspected environment is
`/private/tmp/ashlar-r16-truss048-readonly-compat-312/bin/python`, Python
3.12.14, with `truss-toolkit==0.0.48` and psycopg 3.3.6. Its installed
distribution metadata does not include the `[local]` extra. The wheel declares
that extra as `pgserver-pg18==18.6.1` (bundled PostgreSQL server 18.6), `fasteners==0.20`,
`platformdirs==4.12.4`, and `psutil==7.2.2`; a future fresh-cluster packet must
pin and verify these runtime distributions separately.

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
source. Until the questions in `source-authority-inputs.json` have evidence and
answers, any future source created this way must be labeled
`local_development_authority`; it must not be represented as production source,
production acceptance, or source-owner custody.

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
`.48`→`.51` migration. The seed request may be reused only after the new source
owner confirms its semantics and a new scope receives its own approval.

R21 rejected the retained R16/R20 PostgreSQL data copy because 13 required
PostgreSQL 18 directories are missing, including paths whose contents may be
non-empty; the copy is not an input to this fresh-source proposal. The clean
fresh-cluster route avoids relying on those bytes, but it does not resolve source
authority or qualify the `.48`→`.51` migration. Migration must be proposed as a
separate reviewed one-shot scope after a new `.48` source, acceptance, consumer,
checkpoint, and seed are captured and independently verified.

No PostgreSQL process, source mutation, feed read, migration, Spark/Delta,
publication, ACK, or cloud action was performed while preparing this packet.
