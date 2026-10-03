---
ddx:
  id: ashlar.resource.graphar
  type: resource-summary
  activity: discover
  status: draft
  authoring:
    home: repo
---

# Apache GraphAr

## Source

- URL: [Apache GraphAr](https://graphar.apache.org/)
- Accessed: 2026-10-03
- Revision: live documentation; no release pinned or software executed.

## Summary

Apache GraphAr is an Apache Software Foundation project defining a graph data file format and libraries for storing and retrieving graph data.

## Relevant Findings

- Its format uses chunked, columnar storage and adjacency representations.
- The project provides Spark and PySpark libraries alongside other language libraries.

## HELIX Usage

Informs Ashlar's product vision, initial concerns, and the prior-art assessment
in `../research.md`. Use it as evidence for framing questions.

## Authority Boundary

Evaluate adopting or mapping to GraphAr before inventing a physical graph file format. The overview establishes a candidate, not suitability for an unselected Databricks runtime or Ashlar workload.

## Review Checklist

- [x] Source URL and access date are present.
- [x] Summary is concise and source-faithful.
- [x] Findings are relevant to the framing questions.
- [x] Usage and authority limits are explicit.
