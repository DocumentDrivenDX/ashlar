# R04 B source-byte reuse candidate (read-only analysis)

Status: inert candidate analysis only. No B source was prepared or accepted, and
no PostgreSQL, Spark, source registration, feed, destination, ACK, or source
mutation was performed. This note does not authorize B execution or establish
B authority.

## Finding

The two-source contract requires A and B to have independently accepted Truss
source installations, feeds, epochs, namespaces, transaction inventories,
ordinary source sessions, and protected ACK scopes. It requires distinct native
source identities even when local IDs, revisions, or feed positions overlap.
It does not require distinct UMF document content. Therefore the exact original
UMF 0.10.0 document bytes accepted for A are a grounded candidate for separate
acceptance in B, provided B gets its own installation and provider-issued
registration/source identities and the complete B report/view is read back.
Reusing the bytes would not reuse A's acceptance or grant B source authority.

This conclusion follows from the fresh current-core two-source amendment in
CONTRACT-001. CONTRACT-003 further requires exact source/catalog pins and
independent accepted-source qualification; neither contract says A/B models
must differ. No distinct model is required by the reviewed contract text. Any
R04 schedule or source-content suitability decision remains open until the
complete B source and source transaction inventory are reviewed.

## Proposed B-specific origin bytes (unapproved)

`b-origin-candidate.json` contains one exact candidate origin payload. Its
canonical encoding for this packet is UTF-8 compact JSON with the keys in the
shown order and one trailing LF (141 bytes). SHA-256:
`2e722d0a3c3124d2df2ff4434968de412ef6ad767d4edfd889e160f85a369247`.

```json
{"actor":"ashlar-r04-b-source-acceptance","reason":"independent R04 B source acceptance using the retained original UMF 0.10.0 model bytes"}
```

This candidate names a separate B/R04 purpose and does not reuse A's
`ashlar-truss045-r03` origin. It is **not approved, accepted, or authorization**
for B. An owner must approve or replace this exact payload before any B
acceptance or source effect; offline byte-preserving preparation may proceed
without that approval. The B execution approval may include this exact origin
payload; no separate approval is implied. A changed payload requires a new
exact-byte digest. No approval is recorded by placing this candidate in the
packet.

## Retained original bytes

The exact source used by A's 0.0.48 r4 acceptance is retained at:

`/private/tmp/ashlar-truss045-real-source-20261021/model/source.umf.json`

SHA-256: `498a82f313fd3ceaeb6195e133b403aa829acfcda7cec676a33a5790a57d456b`

The original origin bytes are retained at:

`/private/tmp/ashlar-truss045-real-source-20261021/model/acceptance-origin.json`

SHA-256: `a16b9405b213649ebfbda85ce0bf47877646eccb93b09176190be32bcb674fed`

The source path and both digests are pinned in the r4 acceptance configuration
and subsequent read-only reconciliation configurations. The adoption record
reports that Truss 0.0.48 accepted document
`ashlar-current-045-lifecycle-20261021`, document revision
`ashlar-current-045-lifecycle-r1`, at catalog revision 1, and r5-06's public
`accepted_document` returned bytes equal to that source path. Its complete
current view contained 11 definitions. This establishes A's retained source
bytes and acceptance only; it does not establish B acceptance.

The repository file
`docs/helix/04-build/evidence/truss-current-catalog-0.10-20261021/accepted-source.umf.json`
is a different 0.10 example (`typed-graph-example`), SHA-256
`19ab3cc44efd769346b88fb7c5dc6792b59fd0057ed8846e4a0eec744f8e716b`. It is
not a substitute for the retained A source and must not be used as B evidence.
The A source currently has no copied, hash-verified repository-local source
file in the R04 B packet; the byte source of record is the retained `/private/tmp`
path above. Preserve that custody limitation in any B preparation.

## Origin and provider preconditions

The retained origin JSON is exactly
`{"actor":"ashlar-truss045-r03","reason":"fresh named UMF 0.10 source acceptance and bounded local graph lifecycle preparation"}`.
It is A/R03-specific. No B-specific origin has been approved or retained. The
existing origin digest must not be described as B authorization. Before a B
acceptance attempt, a B-specific origin payload and actor/reason must be
explicitly authorized, frozen, and hashed; the candidate source document bytes
can remain unchanged.

The B packet's observed Truss API sequence requires `prepare_catalog` with
`source_version="0.10.0"` before source transaction/acceptance, followed by
`accept_in_transaction` using that same prepared object, caller transaction,
request key, exact `origin_bytes`, expected catalog revision, and expected
document set. B must then retain its provider-assigned registration/revision
and publicly read back the accepted report and complete catalog view. A draft,
preparation result, A's report/view, or copied installation is not B
acceptance. The exact Truss 0.0.49 package and runtime pins, accepted API
preconditions, isolated B installation controls, B registrar/session binding,
and B reader role remain subject to the existing inert installation packet.

## Remaining gates

- Re-verify both retained byte hashes from the original locations immediately
  before any B preparation. If either retained file is unavailable or changed,
  stop; do not substitute the repository example or regenerate the source.
- Review and explicitly approve or replace the proposed B-specific origin
  bytes; no approved B origin is evidenced here.
- At an authorized future B attempt, use a B-owned private attempt/evidence
  directory outside the public repository. Read the retained source file as
  bytes, verify its recorded SHA-256, copy those exact bytes into the B-owned
  custody directory without parsing or reserialization, set restrictive owner
  access, and verify the copy hash before passing those bytes to preparation
  and acceptance. Record both absolute paths, byte counts, hashes, owner and
  retention policy in the private attempt pins. Preserve the original retained
  A source unchanged; do not commit either copy to the public repo. If the
  original is unavailable or a hash differs, stop and request a newly
  authorized authentic source handoff rather than reconstructing it from a
  parsed model or example. Keep B origin bytes in the same private B custody
  directory with their separate digest and approval record.
- Confirm the same model is suitable for the intended B transaction inventory
  and preserve full B catalog semantics in the accepted report/view.
- Complete an independently isolated B Truss installation and acceptance with
  B-owned identities, then capture public accepted-source, report, and complete
  view evidence. This note supplies none of those results.
- Keep R04 schedule freeze and execution blocked on all other source, feed,
  consumer, ACK, request, and destination evidence required by the contracts.
- One offline preparation pass succeeded through the public top-level
  `truss.prepare_catalog` export; its prepared artifacts remain in private
  `/private/tmp` custody. The `CatalogReferenceAssembly` instance route was not
  used because its constructor requires a connection-backed `PsycopgHost`.
  See [the preparation assessment](b-offline-preparation-assessment-20261023.json).
  This is UMF preparation evidence only, not B acceptance, installation
  authority, source authority, or execution authorization.

## Governing evidence

- `../../../../02-design/contracts/CONTRACT-001-publication-boundary.md`, “Fresh
  current-core two-source acceptance amendment”.
- `../../../../02-design/contracts/CONTRACT-003-delta-graph-tables.md`, UMF model
  and accepted-source requirements.
- `../truss-0.0.48-adoption-preparation.json`, A r4 acceptance and r5-06 public
  byte-exact readback.
- `README.md` in this directory, B API observations
  and isolation/precondition proposal.
