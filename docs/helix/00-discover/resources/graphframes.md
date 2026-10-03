---
ddx:
  id: ashlar.resource.graphframes
  type: resource-summary
  activity: discover
  status: draft
  authoring:
    home: repo
---

# GraphFrames

## Source

- URL: [GraphFrames](https://graphframes.io/04-user-guide/01-creating-graphframes.html)
- Accessed: 2026-10-03
- Revision: live documentation; no release pinned or software executed.

## Summary

GraphFrames is a community-maintained graph processing library built on Apache Spark, the Apache Software Foundation distributed processing engine.

## Relevant Findings

- The creation guide describes vertex and edge DataFrames, using `id` for vertices and `src`/`dst` for edge endpoints.
- Additional columns can carry vertex and edge attributes.

## HELIX Usage

Informs Ashlar's product vision, initial concerns, and the prior-art assessment
in `../research.md`. Use it as evidence for framing questions.

## Authority Boundary

Use the documented input shape to assess a candidate consumer during framing. It does not establish identity semantics across sources or Ashlar compatibility. Runtime and package versions still need selection and testing.

## Review Checklist

- [x] Source URL and access date are present.
- [x] Summary is concise and source-faithful.
- [x] Findings are relevant to the framing questions.
- [x] Usage and authority limits are explicit.
