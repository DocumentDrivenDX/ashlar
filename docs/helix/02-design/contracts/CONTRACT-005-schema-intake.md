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

This initial component produces a shared intake artifact through pinned source
APIs. It does not yet persist a Truss schema_doc or an Ashlar UC schema registry.
Package distribution and signature/custody across machines remain future wiring.
