# Truss feed mapping candidate — 2026-10-06

Governed by Ashlar CONTRACT-003 and proposed ADR-001. Source evidence is draft Truss layout 0.2, CONTRACT-001/002/006 and storage-layout.sql at `spec/change-feed-and-groups`, `6d87fce`. This is an adapter design proposal, not a transport implementation or support claim. UMF interpretation remains deferred.

## Native boundary

A reader selects committed journal rows below the source safe watermark and sorts numerically by `(xid,seq)`. Xid is an unsigned-range decimal string; seq is a signed BIGINT decimal string. Compare parsed exact integers, never string lexical order or floating-point values. The source epoch identifies one durable database lineage and must change on a reseed/reset that invalidates cursor identity. Source-system identity is configured independently of a catalog/schema display name.

Batch boundaries close complete xids. Pagination may split a transaction physically, so the reader buffers until it has established all eligible rows for the xid before advancing its durable consumer cursor. This is an adapter choice to keep published state from exposing half a source transaction. A safe watermark blocked by an old transaction is reported separately from publishable backlog. Statement timestamp `at` remains exact source text; it is not a commit time.

Revision documents precede dependent changes; source/provenance and reservation records accompany or follow their related change as the draft feed specifies. Their delivery association and extraction snapshot must be established by the real reader: the source tables do not have journal-style `(xid,seq)` columns. Do not invent positions for them or acknowledge their coverage from journal progress alone.

## Lossless record capture

Preserve a versioned exact transport envelope in source_record before deriving current/history tables. The envelope carries the native record kind, native key, original cursor where one exists, exact JSONB-as-text fields, flags distinguishing SQL NULL from JSON null, revision document bytes and all unknown fields. Encoding document bytes as base64 is permitted only with a checked original byte digest. Digest the stored UTF-8 envelope bytes, not re-rendered decoded JSON. The tested hex/UTF-8 SQL path preserves nested escapes; a bound-parameter production writer still needs equivalent evidence.

Candidate delivery IDs are compact JSON arrays of string components, avoiding delimiter and numeric coercion. They are publisher-enforced logical identities, not declared native keys:

| Kind | Candidate components after kind tag | Qualification |
| --- | --- | --- |
| change | xid, seq | Preserve entity/version/property replay key as well; native tuple origin controls capture identity |
| revision | rev | Include all documents and origin; content conflict at same revision refuses |
| source | entity kind, entity id | Source DDL declares one immutable provenance row per imported record |
| reservation | entity kind, type, key number, exact key text, former entity id, ver | Reader must prove complete coverage and replacement/timing behavior; the source primary key alone is insufficient to preserve successive reservations |
| unknown | versioned producer delivery identity | Retain exact envelope; refuse qualification when stable identity is unavailable |

Feed and epoch namespace every delivery ID. Capture retries require exact kind/cursor/payload/revision equality. Received time and publisher apply IDs are ingestion metadata and do not change content identity. Preserve unknown operations without interpreting them; stop current-state publication if the unknown meaning could change the answer.

## Projection into existing tables

| Source meaning | Delta projection | Requirement |
| --- | --- | --- |
| o/e entity kind | node/edge | Declared reversible mapping |
| entity id and entity type | native id and type_id/rel_type_id | Exact signed integers; edge endpoints require typed source data |
| ver | entity_version | Record version after the change, as specified by draft journal; do not aggregate unrelated property versions |
| prop_id and old/new JSONB | property_id and exact old_json/new_json plus presence flags | Transport must explicitly distinguish absent SQL NULL from explicit JSON null |
| op | operation | Preserve native operation names; synthetic r69 `property` is not a native Truss operation |
| rev and at | schema_revision and source_time_text | Preserve revision identity and original time text |
| origin and unknown fields | complete source_record envelope | Not representable losslessly in 0.2 property_journal alone |
| xid,seq | original cursor in source_record and publication progress | Never flatten into scalar source_position |

The scalar `source_position`/`event_ordinal` profile used by the existing synthetic fixtures cannot be advertised as a native Truss tuple cursor. A source-aware profile revision must define a direct journal-to-raw-record reference and native tuple progress before real feed ingestion. The synthetic apply_batch_id join is not a substitute for that reference. Current row origin and journal origin must remain independently recoverable after replays and later publications.

Full current carriers require transaction-consistent source rows or a proved reconstruction algorithm. The draft create/delete journal envelopes contain props/retained, not all node logical-key/root or edge endpoint/order fields; therefore journal payload alone is not proved sufficient to rebuild the current graph. Row-home properties, retain/rebind/transform and revision transitions need explicit completeness evidence. Never read latest source current state and label it as an earlier feed boundary.

## Recovery and retention

Persist raw capture and stage identity, apply under a qualified fenced publisher protocol, resolve actual table versions, validate the graph and then publish the immutable vector. Only after durable publication/recovery state may the source feed_consumer position advance. Replayed capture after lost acknowledgement is expected. On unsupported revisions or operations, retain captured bytes but leave the prior published vector and acknowledged cursor unchanged.

History retention remains unspecified. The draft source contract protects rows needed by registered consumers; gold checkpoint and snapshot reseed procedures need their own evidence. No purge, source-consumer removal or larger-scale compute is authorized by this design.

## Required next evidence

Implement and verify the source-profile revision and direct raw-record references, then test a transaction split across reader pages, delayed older xid, revision-before-change, provenance/reservation coverage, bootstrap handshake and crash before/after source acknowledgement. These controls must use an actual source implementation before a native Truss adapter claim. Existing Delta carrier/replay results prove storage primitives only.


## Newer working-draft reconciliation

A later worktree audit found uncommitted CONTRACT-006 and wire contracts beyond commit `6d87fce`. [Source evidence index](out/truss-working-draft-evidence.json) records exact hashes and sizes. These drafts supersede the older commit's event-identity and checkpoint assumptions for compatibility planning; they are not native implementation evidence. The prior candidate delivery IDs for side records are therefore insufficient to qualify the complete feed.

The newer draft requires complete transaction manifests with independently qualified membership/prerequisites, original payload pins and original configuration context. Change identities include trusted epoch/feed profile/record kind/xid/seq. Side facts require complete stable membership/checkpoint identities; revision number or record ID alone is insufficient. A complete downstream boundary may be a verified seed, transaction or coverage boundary, not a highest journal tuple. Preserve manifest/member/prerequisite bytes in the raw carrier without pretending that stored bytes establish producer truth.

Worker identity binds source epoch, feed profile, scope, consumer, trusted registration identity and trusted generation. Generation acquisition and downstream installation are separate steps. Downstream application must atomically validate the installed generation and persist complete effects, deduplication and durable applied boundary; lease expiry alone is not fencing. Replacement must reconcile durable downstream state before applying. Existing publisher_fence and apply_receipt fixtures do not prove this protocol.

Acknowledgement requires host-established proof-verifier custody and independently observed committed application/interval evidence. JSON markers, content hashes, received source manifests or an Ashlar publication row cannot issue acknowledgement authority by themselves. Failed/lost acknowledgement preserves downstream state and replay protection; protected source retention advances only after confirmed source checkpoint commit. Source and downstream are not a distributed transaction.

Compatibility disposition: 0.3's generic exact source_record and cursor/reference columns can retain these additional draft record bytes. The current publication/fence/receipt protocol is unqualified for the newer complete-feed worker profile. Source progress JSON may preserve the exact versioned boundary and context but does not make them trusted. Signed BIGINT fence epochs also require an explicit range qualification when binding exact textual worker generations; refuse unsupported values instead of narrowing. Full canonical structural extraction, bootstrap activation, permissions and source completeness remain separate evidence requirements.

Next implement a qualified complete-feed publisher only against an actual producer/worker runtime and selected Delta transaction/recovery profile. No producer runtime was found in the scoped source inventory; existing Truss application evidence checks only JSON wire shape. Until then, the available safe work is versioned mapping/protocol design and scoped native Delta primitives, with no end-to-end source-support claim.
