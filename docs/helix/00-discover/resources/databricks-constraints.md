---
ddx:
  id: ashlar.resource.databricks-constraints
  type: resource-summary
  activity: discover
  status: draft
  authoring:
    home: repo
---

# Databricks constraints

## Source

- URL: [Databricks constraints](https://docs.databricks.com/aws/en/tables/constraints)
- Accessed: 2026-10-03
- Revision: live documentation; no release pinned or software executed.

## Summary

Databricks is the data platform selected by the owner; its AWS constraint documentation distinguishes enforced checks from informational relationships on Delta Lake tables.

## Relevant Findings

- The page documents enforced NOT NULL and CHECK constraints.
- Primary and foreign key constraints are informational, not enforced; the page places their availability on Unity Catalog and Delta Lake tables at Runtime 13.3 LTS and later.

## HELIX Usage

Informs Ashlar's product vision, initial concerns, and the prior-art assessment
in `../research.md`. Use it as evidence for framing questions.

## Authority Boundary

Use this distinction when framing validation and integrity expectations. This AWS documentation, updated 2026-09-11, does not validate another cloud, runtime configuration, uniqueness, referential integrity, or any Ashlar implementation.

## Review Checklist

- [x] Source URL and access date are present.
- [x] Summary is concise and source-faithful.
- [x] Findings are relevant to the framing questions.
- [x] Usage and authority limits are explicit.
