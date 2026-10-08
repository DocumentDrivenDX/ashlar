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
