---
ddx:
  id: CONTRACT-005
  type: contract
  activity: design
  status: draft
  authoring:
    home: repo
  links:
  - id: CONTRACT-003
    kind: informed_by
  - id: CONTRACT-004
    kind: informed_by
  - id: FEAT-002
    kind: informed_by
---

# Contract: shared UMF schema intake

Proposed ashlar-schema-intake/0.1. Intake validation is not catalog acceptance,
constraint enforcement, schema migration or source-data publication.

The development CLI tools/inspect_umf.ts accepts a trusted UMF source directory,
its exact Git revision and one UTF8 JSON schema document. The source checkout must
be clean, match the supplied revision and supply its existing reader/validator.
Refuse invalid UTF8, invalid UMF or a changed/unpinned validator source. Output
is one JSON artifact containing format, sourceBase64, sourceSha256, sourceBytes,
umfCoreVersion, documentId, validatorRevision and the original validation result.
Use the exact retained bytes as source authority; no reserialized document replaces
them. No private UMF vocabulary or alternative semantic validator is introduced.

Both downstream schema registries must verify the retained-byte digest before
consuming this artifact and perform their own versioned binding/catalog acceptance.
The artifact records validatedStructure and completeInterpretation separately.
Unknown assertions remain retained. A complete UMF validation still does not mean
Truss or Ashlar can enforce every assertion; target bindings must classify native,
engine and unsupported enforcement, and refuse unsupported executable ingestion.
Unknown/breaking schema effects cannot advance a published data revision silently.

A document revision identifier and stable catalog ID mapping must be supplied by
the selected schema registry/source profile, not inferred from this inspection.
Same schema identity/revision with different source bytes is a conflict; original
bytes remain authoritative on replay. Additive compatibility, key/relationship
changes and data conversion need explicit target-specific acceptance rules.

The intake reader and development custody check now retain these artifacts in
a separate PostgreSQL intake store and a UC Delta intake table. Neither writes
Truss schema_doc, accepted catalog head or an accepted Ashlar schema revision.
Package distribution and signature/custody across machines remain future wiring.

## Raw registry surface

The proposed raw registry key is (document_id, document_revision), with explicit
opaque nonempty revision supplied by the caller. Its eight required fields are
document_id, document_revision, source_base64, artifact_base64, source_sha256,
artifact_sha256, validator_revision and complete_interpretation. Base64 retains
exact source and intake-artifact bytes; SHA256 is lowercase hexadecimal over
those decoded bytes. Preserve the full diagnostic artifact, including unknown
fields and validation warnings. validatedStructure and validation.valid must
both be true; completeInterpretation must agree with validation.complete.
The reader checks source identity/core version and exact byte length/digest.
Duplicate JSON members, nonfinite numbers and malformed UTF8 refuse.

An identical complete row replays without replacing the original. Any changed
field under an existing qualified key refuses SCHEMA_INTAKE_CONFLICT and leaves
the original intact. A later revision is a separate retained artifact, not an
automatically compatible or accepted schema. For example revisions 1 and 2 of
truss-weft-source-review-fixture retain the required label and additive optional
caption respectively; neither advances an accepted catalog or publication.

The caller must supply the trusted validator source pin and independently trust
the original artifact producer/custody. Comparing an artifact's declared pin
does not authenticate it or rerun UMF. This reader is a custody verifier, not an
alternative UMF semantic validator. No signature or remote intake authority is
claimed by the local development profile.

## Native development evidence and boundaries

The 2026-10-08 bounded native check stored three synthetic documents in
ashlar_intake.document on the isolated PostgreSQL17.9 sandbox and
client_dev.ashlar_layout_v03_20261006_r73.schema_intake_20261008 on UC Delta.
Both enforce decoded-byte hashes natively and preserve exact artifact/source
bytes through replay and a valid-digest conflict attempt. PostgreSQL uses a PK;
Delta MERGE is qualified only with one serialized development writer and does
not establish concurrent uniqueness. Application fencing/privileges and a
production immutable registry are not delivered. The check uses administrative
development authority, with PUBLIC access revoked on the PostgreSQL intake
schema/table. Accepted catalog schema, stable binding IDs, constraint enforcement
and runtime/feed operations remain separate outstanding work.

The first Delta inline-CHECK DDL failed terminally with no table creation; the
resumed check verified the empty PostgreSQL store and created Delta with separate
ALTER TABLE ADD CONSTRAINT statements. Original native statement/SQL receipts
and the passed summary remain under the spike's out/native/schema_registry_20261008.
The runner refuses silent IF NOT EXISTS reuse. A failed/uncertain future write
requires inspection of its existing native handle/custody before any replay.

## Stable identity planning boundary

The proposed pure planner accepts a complete selected identity inventory, complete
prior mappings, family highwaters and the expected native catalog head. Native
acceptance must read these under exclusion, revalidate the resulting plan and
persist allocation with the complete accepted semantic/catalog/report effects.
A returned plan is not a committed allocation or acceptance token.

Identity parts are exact opaque UTF8 strings; no case-folding or Unicode
normalization occurs. Type identity is (document, module, authored element);
property identity is (document, owning module, owning Record element, field
module, field element); proposed relationship identity is (document, owning
module, owning Record element, authored relationship). Display names, source
ordinals and document revisions are definition provenance, not lineage. The
relationship tuple remains subject to the selected Truss/UMF relationship
extraction profile; this planner does not extract relationships or admit
relationship semantics. Existing mappings must come from authoritative catalog
custody, never from the synthetic Weft source-review IDs.

Each family has a separate signed-int32 positive ID allocation range. Preserve
all prior IDs, including retired identities; allocate fresh IDs strictly above
the supplied highwater. Missing mappings do not authorize filling highwater
gaps. Duplicate identity/ID mappings, invalid ranges, incomplete highwater
inventory or active members without an active owning type refuse. A complete
inventory omitting an active prior identity plans retirement; same qualified
identity reactivation keeps its ID and never restores grants implicitly. A
distinct authored identity gets a fresh ID. Exhaustion refuses a new allocation,
but does not prohibit validation/reactivation of an existing identity.

Source `src/ashlar/catalog.py` supplies this planner. It does not validate field
meaning, key equality, schema compatibility, data transforms or native effects.
Its caller must first admit those under the target profile and supply complete
selected ownership. Host tool `tools/project_umf_identities.ts` invokes the
pinned existing UMF reader/validator and records every authored element tuple
with original source pointers and validation diagnostics. It retains source
kind assertions verbatim and infers no type/property/relationship bindings.
The additive example yields three authored elements; executable semantic
projection and atomic catalog acceptance remain outstanding.

## UMF interpretation and selected binding planner

Host tool tools/inspect_schema_semantics.ts invokes the pinned UMF reader and
existing kind, nullability, cardinality, facets, keys, relationship and schema-
property interpretation APIs. The report retains exact source bytes/digest and
original complete inspection receipts. Unknown and legacy meanings stay as UMF
returned them; no consumer token alias or invented author receipt is introduced.
Schema-property inspection at the selected source requires core 0.8.0 and
refuses 0.7.0. Preserve that actual unavailable result and original error rather
than presenting missing schema properties as a complete interpretation.

Proposed ashlar-string-record-binding-plan/0.1 is a target consumer planning
profile for explicit core 0.7 singleton string Record members. It checks complete
original element receipt membership, source/identity/pointer correspondence and
digest before deriving owner-qualified type/property identities. Source kind
must be known Record/Field; selected Field scalar family is string, cardinality
is known one, and availability is known required or absent-allowed. Original
source and inspection bytes, names and all warnings remain retained. This
consumer requires independently trusted original interpretation custody: matching
a declared validator pin is not authentication or UMF receipt recomputation.

Unsupported kind/value shape, unresolved/duplicate member, unknown availability,
facets, keys, extension meaning, relationship or other unbound assertion blocks
this selected executable profile with source pointers and none enforcement.
Known supported members are marked engine-unimplemented, not database-enforced.
No candidate plan grants data-write authority or advances schema/head/publication.
This first binding profile does not reduce the end-to-end goal: broader values,
keys and relationships remain required bindings rather than omitted requirements.
The native accepted profile must compose their original semantic and effect
obligations before claiming complete runtime support.

The original revision-2 example caption asserts optional; the real pinned UMF
nullability inspector returns unknown/value optional. Its binding plan therefore
blocks at the original caption nullability pointer. A separate revision-3 fixture
explicitly asserts absent-allowed and yields a candidate binding plan. It is a
new authored example, not reinterpretation or rewriting of retained revision 2.
Neither fixture has native accepted catalog status. Unknown root assertions and
newer schema properties remain retained and block this selected binding when
present; supported target interpretation cannot be inferred from structural
validity or the presence of a source field.

The runnable register_umf.py path invokes actual pinned clean UMF reader/validator
code and retains raw documents/complete receipts through DeltaSchemaRegistry.
Native isolated v1 and additive v3 source/artifact readback is byte-exact; both
retain incomplete interpretation. Registration is immutable by document/revision,
with source identity and validator digest verified, mandatory writer policy and
UUID/schema/grant checks. Native semantic/catalog acceptance, stable allocation
and schema barriers remain independent required steps; no raw registry row
authorizes data ingestion or advances Truss schema_head.

The selected StringRecordPolicy now enforces interpreted required/absent-allowed
singleton string fields at the host graph-planning boundary, keyed by complete
explicit trusted catalog bindings. Unsupported assertions/nulls/arrays/unknown
properties and source/revision drift refuse. Actual native acceptance must prove
the supplied IDs/revision against retained catalog authority; test fixture IDs
do not establish that authority. Relationships, broader values/facets/keys and
Truss native enforcement are still unimplemented requirements.
