---
ddx:
  id: ashlar.evidence.r39-nullability-reconciliation-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
---

# R39 explicit-null admission reconciliation

R39 reached the selected seven-change seed page and stopped before its page-2
Delta publication or source ACK. The original failure was Ashlar’s scalar
admission check: `Original scalar family/nullability differs`.

A bounded read-only Truss 0.0.52 public feed reread returned the exact selected
page SHA-256 `0248fd1607b971f267df71b1262668930a96925a767855dd3400fb0ecd1a6456`.
Its prior cursor matched the retained genesis ACK checkpoint
`8e370e1497197b93029ff7f5287d9102802a098fbc4ea02b1e6bf20339841f9f`; the page
contains seven changes. The page report is revision 1, registration
`4356b0ee-2d21-4a39-9182-b82c66dffd97`, report digest
`466a3c0f4f0223499f9f1ba2e9c4bf6a73d74ddcbbc3b9275c89276e6efd502c`. Its
accepted artifact SHA matches `r28-model.umf.json` at
`b908c0dd6a0da65c864c0debd0c793d8ab06a933546f34050480909c5c9349f2`.

Change 2 is a `Person` object with property ID `3` and value `{"kind":"null"}`.
The accepted model maps property ID `3` to `quantity`, declared as an integer,
one-valued, `absent-allowed` field. This is an explicit null carrier, not an
absent property. CONTRACT-003 distinguishes a missing member from JSON null;
UMF’s `absent-allowed` declaration admits either presence state. Ashlar must
preserve the delivered null while rejecting null on required fields and keeping
non-null family/facet checks unchanged. The adapter change and focused tests
implement that boundary.

The destination still has only the genesis publication: all four graph-role
heads remain at vector v1 and match the retained native readback, with empty
row bags; manifest v2 records only the genesis page SHA `942a3257…f8a9a0`.
The read-only SQLite journal reports six operations, one plan, one source plan,
and one publication artifact. R39 did not attempt an ACK. The post-read PG18
cluster is stopped and rebaselined at 1,261 files / 65,208,181 bytes with tree
SHA-256 `06165099b313b5470120140f9e8efbe6a670a7a9c3045ec57016148badb550ce`.

The exact public page bytes are retained in [r39-public-feed-page.json](r39-public-feed-page.json).
The machine-readable reconciliation is [r39-nullability-reconciliation.json](r39-nullability-reconciliation.json)
and its external receipt SHA-256 is `d4903948a93d87fad3e987de8a1aa6063db9a623077460b2534be38bf821eb61`.
The native failure and prior R39 receipts remain unchanged. This evidence is
local-development only and does not establish production source authority.
