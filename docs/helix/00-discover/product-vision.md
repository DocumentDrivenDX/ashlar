---
ddx:
  id: ashlar.product-vision
  type: product-vision
  activity: discover
  status: draft
  authoring:
    home: repo
  links:
  - id: ashlar.resource.graphframes
    kind: informed_by
  - id: ashlar.resource.graphar
    kind: informed_by
  - id: ashlar.resource.databricks-constraints
    kind: informed_by
---

# Product Vision

## Mission Statement

Ashlar gives data platform engineers a common structure for graph nodes on
Databricks so independently produced datasets can be understood and reused.

## Positioning

For data platform engineers sharing graph-shaped datasets between ingestion
pipelines and analytics on Databricks, Ashlar is a shared graph data contract.
Its proposed benefit over independently specified vertex and edge tables is
agreement on what a node means across producers and consumers. This audience
and benefit are hypotheses, pending a first-user example.

## Vision

Engineers can combine graph data from different producers with explicit,
reviewable agreements about identity, properties, and compatibility. Consumers
can tell which assumptions hold before relying on a dataset.

**North Star**: Two independent producers supply data to one agreed consumer
using the same versioned contract without producer-specific structural remapping.

## User Experience

In an illustrative future session, a platform engineer brings customer and
account datasets from two pipelines. The engineer describes how each source
maps to the agreed node contract and checks the result. A consumer receives
consistent records; conflicts and unsupported mappings remain visible for review.
The engineer can identify which contract revision each producer used. This is a
proposed experience, not implemented functionality or an approved feature list.

## Target Market

| Attribute | Description |
| --- | --- |
| Who | Databricks platform teams maintaining at least two graph-data producers and one shared analytics consumer; first team still to identify. |
| Pain | Repeated negotiation of identity and structure across independently defined datasets; assumption A2 in the discovery input. |
| Current Solution | Individually specified tables, potentially exposed through GraphFrames vertex/edge DataFrames. |
| Why They Switch | A repeatable contract could reduce integration rework; interviews and a pilot must establish the benefit. |

## Key Value Propositions

| Value Proposition | Customer Benefit |
| --- | --- |
| Shared structural meaning | Consumers can reuse data without relearning each producer's conventions. |
| Explicit compatibility boundaries | Engineers can see unsupported mappings before trusting combined data. |
| Reviewable evolution | Producers and consumers can agree on changes before adoption. |

## Success Definition

These are proposed strategic measures for the first 12 months after a pilot
begins. The owner must confirm targets during framing; no baseline exists yet.

| Metric | Target |
| --- | --- |
| Independent adoption | Two producers and one consumer pass the same agreed contract checks, verified by a reproducible pilot. |
| Reuse | Zero producer-specific structural remapping inside that consumer, verified by reviewing its integration code. |
| Meaning preservation | Zero unexplained value loss across the pilot's agreed mapping corpus, verified by input/output comparisons. |

## Why Now

The owner has identified a need to standardize graph nodes on Databricks before
choosing a structure. The [initial research](research.md) identifies GraphFrames,
a Spark graph-processing library, and Apache GraphAr, a graph file format, as
existing candidates to reuse. Databricks documents an enforcement boundary that
framing must account for. These are reasons to investigate a shared contract now;
market urgency and customer demand remain unvalidated.

## Review Checklist

- [x] Mission names the user, problem, and proposed approach.
- [x] Positioning identifies the current alternative and proposed benefit.
- [x] Vision describes an end state and a measurable north star.
- [x] A concrete scenario, target users, and switching hypothesis are recorded.
- [x] Proposed metrics name outcomes, measurement methods, and a time horizon.
- [x] Why Now distinguishes owner direction from research and assumptions.
- [x] Requirements and technical choices remain for downstream artifacts.
- [ ] Owner approval and first-user validation are pending.
