---
ddx:
  id: ashlar.resource.umf
  type: resource-summary
  activity: discover
  status: draft
  authoring:
    home: repo
---

# UMF integration evidence

## Source

- Repository: [DocumentDrivenDX/UMF](https://github.com/DocumentDrivenDX/UMF).
- Inspected 2026-10-03: clean local checkout `/home/erik/Projects/umf`, commit
  `b9082ee97d4114580d508e0a71557d9ebf56c931`; not verified as current remote master.
- Read `README.md`, `package.json`, and
  `docs/helix/02-design/contracts/CONTRACT-042-physical-binding.md` at that revision.
  Remote README access failed; the local source is the evidence used here.

## Summary

UMF is DocumentDrivenDX's experimental machine-readable metamodel and schema
interchange fabric. It separates logical meaning from versioned physical binding
and retains source material with explicit projection limits.

## Relevant Findings

- The package version is 0.1.0; this is distinct from core/vocabulary versions.
- CONTRACT-042 describes `umf.binding` 0.2.0 / `umf-binding-2` references to
  stable relationship IDs in experimental core 0.7.0. Ashlar must pin each
  independently rather than use the brief's approximate “core around 0.6.”
- Physical binding already separates logical IDs, column/embedded choices,
  relationship storage, and index declarations. Reuse before extending.
- Its Delta 3.2 evidence emits clustering metadata for an existing table;
  it explicitly does not prove clustered files or multi-table Delta generation.
- Relationship binding migration evidence does not establish relationship
  storage generation or native referential enforcement.

## HELIX Usage

Informs concerns and the UMF integration boundary in Ashlar's vision/PRD.
FEAT-001 must inspect the pinned core and extension contracts before defining
an Ashlar profile. This note is evidence discovery, not a dependency selection.

## Authority Boundary

No UMF tests were rerun. The upstream contract itself is a draft with scoped
implementation claims. No upstream files were changed; no core rename, action
schema, graph generator, or Databricks support is inferred.

## Review Checklist

- [x] Exact inspected revision and files recorded.
- [x] Package, core, vocabulary, and target versions distinguished.
- [x] Capability and evidence limits retained.
