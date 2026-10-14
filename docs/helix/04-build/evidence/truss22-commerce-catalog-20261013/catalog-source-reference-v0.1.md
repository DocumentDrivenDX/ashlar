---
ddx:
  id: truss.catalog-source-reference.v0.1
  type: contract
  activity: design
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: informed_by
---

# Catalog source references — selected public Python profile

SDK0.0.22 selects `truss-fresh-catalog-support/0.14.0` for original UMF0.8.0. The pin is `1d89c2cdaa468a0ca01f837624787ca678aa6b4dafed6417018e63c0ebcfca56`. Its native58 report uses `truss-catalog-registration-report/0.2` and `source.sourceFormat = truss-catalog-source-reference/0.1`. Legacy selections and stored reports retain their original format and bytes.

Each `source.assertionEntries` item retains its qualified owner, definition digest, exact `assertion.sourcePointer`, rule, enforcement and reason. Its `source` contains:

```json
{
  "identity": "original artifact identity",
  "sha256": "original document digest",
  "reference": {
    "interfaceVersion": "truss-catalog-source-reference/0.1",
    "documentId": "original qualified document ID",
    "documentRevision": "original document revision",
    "contentSha256": "original document digest",
    "basis": "original"
  }
}
```

The reference resolves only against the same report's `acceptedInput.documents`, whose artifact retains the original bytes as canonical base64 and their SHA256. It never selects a current document or another report. Identity, revision and both digests must match; decoded bytes must hash to that digest. Native acceptance checks the reference before persistence.

Consumers use the public `resolve_catalog_report_source(report_bytes, entry['source'])`. It returns `CatalogSourceDocument` with exact `content`, `sha256`, `artifact_identity`, `document_id` and `document_revision`. Occurrence locations remain in `entry['assertion']['sourcePointer']`; no pointer rewriting or normalized-document replacement occurs. Unknown extension content remains in the exact document bytes and in retained extension occurrences. Required fields remain represented by original kind/nullability/cardinality/scalar/facet/member assertions; references do not remove inventory entries or claim broader enforcement.

Truss-generated compact observation wrappers have separately identified artifacts. Original UMF producer bytes and pins remain unchanged. Only defined observation/source-artifact positions carry references; arbitrary extension members are never interpreted as references. Original and reversible-target bases remain distinct in preparation. This public native profile persists original-basis references only. The older private TypeScript occurrence/report composition explicitly refuses this selected format; it does not become a second public implementation.

The unchanged commerce fixture passes one supplier/product/relationship slice, including source custody, business-key lookup, exact decimal(18,2) query, committed replay and feed delivery. This is not all-pack qualification. The implementation also admits the document's top-level title as retained metadata.

`prepare_catalog(documents, source_version='0.8.0', bun_executable='bun')` is a public standalone entrypoint. It uses the same original producers and selected preparation as the assembly method and needs no database/host. It returns `PreparedAcceptance`; native acceptance and data compatibility still occur in the caller's transaction. Source0.7.0 remains separately selectable and retains its report format.
